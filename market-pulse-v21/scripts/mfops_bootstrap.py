"""Create the first organization, division and platform administrator.

    DATABASE_URL=... python scripts/mfops_bootstrap.py \
        --org "Havel Property Holdings LLC" \
        --division "California" \
        --email you@example.com

THE CHICKEN AND EGG. Every write path in lib/ops/repository.py needs a
Scope, a Scope comes from a session, a session comes from a user, and a
user with any authority has to be created by somebody who already has
more. There is no way to make the first platform_admin through the
application, and pretending otherwise would mean either a default
account (which never gets deleted) or a self-registration route on the
staff portal (which is an open door).

So: a script, run once, by somebody who already has the database URL —
which is the credential that would let them do this by hand anyway. It
grants no authority the operator did not already possess; it just does
it correctly, with the audit rows, the password hashing and the MFA
enrolment that a hand-written INSERT would skip.

It REFUSES TO RUN TWICE against an organization that already has a
platform_admin. A second silent bootstrap is how a forgotten account
with full access ends up in a production database.

Prints the TOTP secret and provisioning URI once. platform_admin
requires MFA (mf_roles.requires_mfa), so without enrolling here the
account is created and immediately unusable.
"""
from __future__ import annotations

import argparse
import getpass
import os
import secrets
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.ops import auth as AU
from lib.ops import clock as C
from lib.ops.migrations import runner as R


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--org", required=True, help="legal entity name")
    ap.add_argument("--division", default="Main", help="first division name")
    ap.add_argument("--email", required=True)
    ap.add_argument("--name", default="", help="full name")
    ap.add_argument("--password", default="",
                    help="omit to be prompted; a generated one is printed "
                         "if you pass --generate-password")
    ap.add_argument("--generate-password", action="store_true")
    ap.add_argument("--migrate", action="store_true",
                    help="apply pending migrations first")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    url = os.getenv("DATABASE_URL")
    if not url:
        print("DATABASE_URL is not set.", file=sys.stderr)
        return 2

    import psycopg2
    conn = psycopg2.connect(url)
    ts = C.TimeService()

    if args.migrate:
        ran = R.migrate(conn, actor="bootstrap")
        print(f"migrations applied: {ran or 'none pending'}")

    cur = conn.cursor()
    cur.execute("SELECT to_regclass('public.mf_users')")
    if cur.fetchone()[0] is None:
        print("The mf_ schema is not present. Re-run with --migrate.",
              file=sys.stderr)
        return 2

    # The refusal. Checked against the whole database rather than one
    # organization: a second platform_admin anywhere means somebody has
    # already done this, and a bootstrap that runs again "just in case"
    # is how a forgotten full-access account appears.
    cur.execute(
        "SELECT u.email FROM mf_user_roles ur "
        "JOIN mf_roles r ON r.id = ur.role_id "
        "JOIN mf_users u ON u.id = ur.user_id "
        "WHERE r.key = 'platform_admin' AND ur.revoked_at IS NULL")
    existing = [r[0] for r in cur.fetchall()]
    if existing:
        print(f"A platform administrator already exists ({', '.join(existing)}).\n"
              f"Refusing to bootstrap a second one — create further "
              f"administrators by signing in as this one and granting the "
              f"role, which leaves an audit trail.", file=sys.stderr)
        return 1

    if args.generate_password:
        password = secrets.token_urlsafe(18)
    elif args.password:
        password = args.password
    else:
        password = getpass.getpass("Password for the administrator: ")
        if password != getpass.getpass("Again: "):
            print("Passwords did not match.", file=sys.stderr)
            return 2
    try:
        AU.hash_password(password)      # validate before writing anything
    except AU.AuthError as e:
        print(f"{e}", file=sys.stderr)
        return 2

    if args.dry_run:
        print(f"would create org {args.org!r}, division {args.division!r}, "
              f"administrator {args.email}")
        return 0

    try:
        cur.execute("INSERT INTO mf_organizations (legal_name) VALUES (%s) "
                    "RETURNING id", (args.org,))
        org_id = cur.fetchone()[0]
        cur.execute("INSERT INTO mf_divisions (organization_id, name) "
                    "VALUES (%s, %s) RETURNING id", (org_id, args.division))
        div_id = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO mf_users (organization_id, email, full_name, "
            "division_id) VALUES (%s, %s, %s, %s) RETURNING id",
            (org_id, args.email, args.name or args.email.split("@")[0],
             div_id))
        user_id = cur.fetchone()[0]
        # An unscoped grant, which mf_user_roles_scope_ck permits for
        # platform_admin and for nothing else.
        cur.execute("INSERT INTO mf_user_roles (user_id, role_id) "
                    "SELECT %s, id FROM mf_roles WHERE key = 'platform_admin'",
                    (user_id,))
        cur.close()

        AU.set_password(conn, user_id, password, ts)
        secret = AU.enroll_mfa(conn, user_id, ts)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"bootstrap failed and was rolled back: {e}", file=sys.stderr)
        return 1

    print(f"\norganization  {args.org} (id {org_id})")
    print(f"division      {args.division} (id {div_id})")
    print(f"administrator {args.email} (id {user_id})")
    if args.generate_password:
        print(f"password      {password}")
    print(f"\nMFA IS REQUIRED for platform_admin. Add this to your "
          f"authenticator NOW —\nit is not readable afterwards, and without "
          f"it the account cannot sign in:\n")
    print(f"  secret  {secret}")
    print(f"  uri     {AU.provisioning_uri(secret, args.email)}\n")
    print(f"Sign in at /ops/staff/login")
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
