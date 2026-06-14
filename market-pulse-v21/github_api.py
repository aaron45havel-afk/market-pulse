"""GitHub REST API helper.

Used by the pipeline /api/pipeline/vercel/create endpoint to spin up
a new private repo for a client prototype, so the 🚀 button can hand
you a `git clone` URL with no detour through github.com.

Env vars:
  GITHUB_TOKEN — personal access token with `repo` scope.
                 Create one at https://github.com/settings/tokens/new
                 → tick the `repo` scope → generate.
"""
from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request


API = "https://api.github.com"


def _token() -> str:
    return os.environ.get("GITHUB_TOKEN", "").strip()


def configured() -> bool:
    return bool(_token())


def slugify(name: str) -> str:
    s = (name or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return (s or "repo")[:90]


def _request(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    token = _token()
    if not token:
        return 0, {"message": "GITHUB_TOKEN not set"}
    url = API + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization":        f"Bearer {token}",
            "Accept":               "application/vnd.github+json",
            "User-Agent":           "market-pulse/1.0 (focusedops.io)",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    if body is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"{}")
        except Exception:
            return e.code, {"message": f"HTTP {e.code}"}
    except Exception as e:
        return 0, {"message": str(e)}


def get_authenticated_user() -> dict | None:
    status, data = _request("GET", "/user")
    return data if status == 200 else None


def create_repo(*, name: str, description: str | None = None,
                private: bool = True) -> dict:
    """Create a new repo under the authenticated user. auto_init=True
    so the repo has an initial commit + default branch, which Vercel
    needs to link cleanly."""
    payload = {
        "name":        slugify(name),
        "description": description or "",
        "private":     bool(private),
        "auto_init":   True,
    }
    status, data = _request("POST", "/user/repos", payload)
    if status in (200, 201):
        return {
            "ok":            True,
            "full_name":     data.get("full_name"),
            "clone_url":     data.get("clone_url"),
            "ssh_url":       data.get("ssh_url"),
            "html_url":      data.get("html_url"),
            "default_branch": data.get("default_branch") or "main",
        }
    return {
        "ok":    False,
        "error": data.get("message", f"HTTP {status}"),
        "raw":   data,
    }
