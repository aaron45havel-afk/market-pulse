"""Vercel REST API helper.

Used by the pipeline to spin up a project + first deployment for a
client when they land in the VERCEL_PROJECT stage.

Env vars:
  VERCEL_TOKEN    — personal token from https://vercel.com/account/tokens
  VERCEL_TEAM_ID  — optional, only needed if the user belongs to a team
                    and wants projects created in that team scope
"""
from __future__ import annotations

import json
import os
import re
import urllib.parse
import urllib.request
import urllib.error


VERCEL_API = "https://api.vercel.com"


def _token() -> str:
    return os.environ.get("VERCEL_TOKEN", "").strip()


def _team_qs() -> str:
    tid = os.environ.get("VERCEL_TEAM_ID", "").strip()
    return f"?teamId={urllib.parse.quote(tid)}" if tid else ""


def configured() -> bool:
    return bool(_token())


def slugify(name: str) -> str:
    """Vercel project names must be lowercase, hyphen-separated, max 100 chars."""
    s = (name or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return (s or "project")[:100]


def parse_github_repo(value: str) -> str | None:
    """Accept either `owner/repo` or a full URL like
    `https://github.com/owner/repo(.git)`. Returns `owner/repo` or None."""
    v = (value or "").strip()
    if not v:
        return None
    m = re.match(r"^([\w.-]+)/([\w.-]+?)(?:\.git)?$", v)
    if m:
        return f"{m.group(1)}/{m.group(2)}"
    m = re.match(r"^https?://(?:www\.)?github\.com/([\w.-]+)/([\w.-]+?)(?:\.git)?/?$", v)
    if m:
        return f"{m.group(1)}/{m.group(2)}"
    return None


def _request(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    """Lightweight Vercel API call. Returns (status, json_body)."""
    token = _token()
    if not token:
        return 0, {"error": {"message": "VERCEL_TOKEN not set"}}
    url = VERCEL_API + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
            "User-Agent":    "market-pulse/1.0 (focusedops.io)",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except Exception:
            return e.code, {"error": {"message": f"HTTP {e.code}"}}
    except Exception as e:
        return 0, {"error": {"message": str(e)}}


FRAMEWORK_ALIASES = {
    "nextjs":  "nextjs",
    "next":    "nextjs",
    "vite":    "vite",
    "react":   "create-react-app",
    "static":  None,    # plain HTML site
    "other":   None,
}


def create_project(*, name: str, github_repo: str | None,
                   framework: str | None = "nextjs") -> dict:
    """Create a Vercel project, optionally linked to a GitHub repo.
    Returns {ok, project_id?, project_url?, error?}."""
    if not configured():
        return {"ok": False, "error": "VERCEL_TOKEN not set on the server."}
    project_name = slugify(name)
    payload: dict = {"name": project_name}
    if framework and framework in FRAMEWORK_ALIASES:
        fw = FRAMEWORK_ALIASES[framework]
        if fw:
            payload["framework"] = fw
    if github_repo:
        repo_full = parse_github_repo(github_repo)
        if not repo_full:
            return {"ok": False,
                    "error": "GitHub repo should be owner/name or a github.com URL."}
        payload["gitRepository"] = {"type": "github", "repo": repo_full}
    status, data = _request("POST", f"/v10/projects{_team_qs()}", payload)
    if status not in (200, 201):
        msg = (data.get("error") or {}).get("message") or f"HTTP {status}"
        return {"ok": False, "error": msg, "vercel_response": data}
    pid = data.get("id") or data.get("projectId")
    # Default production domain Vercel allocates.
    default_url = f"https://{project_name}.vercel.app"
    return {
        "ok":          True,
        "project_id":  pid,
        "project_url": default_url,
        "name":        project_name,
        "vercel":      data,
    }


def trigger_deployment(*, project_name: str, github_repo: str) -> dict:
    """Kick the first build off the linked GitHub repo's default
    branch. Vercel usually auto-deploys when a project is created
    against a repo, but force-triggering doesn't hurt."""
    repo_full = parse_github_repo(github_repo)
    if not repo_full:
        return {"ok": False, "error": "bad github repo"}
    payload = {
        "name":       project_name,
        "gitSource":  {"type": "github", "repo": repo_full, "ref": "main"},
        "target":     "production",
    }
    status, data = _request("POST", f"/v13/deployments{_team_qs()}", payload)
    if status not in (200, 201):
        msg = (data.get("error") or {}).get("message") or f"HTTP {status}"
        return {"ok": False, "error": msg, "vercel_response": data}
    return {
        "ok":         True,
        "deployment": data,
        "url":        data.get("url") and f"https://{data['url']}",
    }
