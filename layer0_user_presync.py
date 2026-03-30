#!/usr/bin/env python3
"""
Pre-sync all Taiga users into a Plane workspace before project migration.

What this script does:
- fetches all Taiga users, memberships, and roles
- creates missing Plane users in the target workspace database
- creates/updates workspace memberships for those users
- preserves a JSON/Markdown report for later production planning

What this script does not do:
- create project memberships (those depend on migrated Plane project IDs)
- touch issue/comment/page migration data
"""

import argparse
import importlib.util
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


ROOT = Path(__file__).resolve().parent
LAYER2_PATH = ROOT / "layer2_update_plane_db.py"
TAIGA_SYNC_PATH = ROOT / "taiga_user_sync.py"


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, str(path))
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


layer2 = load_module(LAYER2_PATH, "layer2_presync")
taiga_sync = load_module(TAIGA_SYNC_PATH, "taiga_sync_presync")


PLANE_WORKSPACE_SLUG = os.environ.get("PLANE_WORKSPACE_SLUG", "").strip()
if not PLANE_WORKSPACE_SLUG:
    raise SystemExit("Missing required environment variable: PLANE_WORKSPACE_SLUG")


def parse_args():
    parser = argparse.ArgumentParser(description="Pre-sync all Taiga users into a Plane workspace")
    parser.add_argument(
        "--report-prefix",
        default=str(ROOT / "runs" / "taiga_global_user_presync"),
        help="Prefix for JSON/Markdown reports",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write DB changes; still produce report",
    )
    return parser.parse_args()


def now_utc():
    return datetime.now(timezone.utc)


def get_workspace_id(cursor, slug: str):
    cursor.execute("SELECT id FROM workspaces WHERE slug = %s AND deleted_at IS NULL LIMIT 1", (slug,))
    row = cursor.fetchone()
    return row[0] if row else None


def strongest_workspace_targets(taiga_users, taiga_memberships, taiga_roles):
    role_by_id = {role.get("id"): role for role in taiga_roles if isinstance(role, dict)}
    role_by_name = {
        (role.get("name") or "").strip().lower(): role
        for role in taiga_roles
        if isinstance(role, dict) and role.get("name")
    }

    email_by_user_id = {}
    taiga_user_rows = {}
    for user in taiga_users:
        if not isinstance(user, dict):
            continue
        email = taiga_sync.normalize_email(user.get("email"))
        if not email:
            continue
        if user.get("id") is not None:
            email_by_user_id[user.get("id")] = email
        taiga_user_rows[email] = user

    targets = {}
    for membership in taiga_memberships:
        if not isinstance(membership, dict):
            continue
        email = taiga_sync.normalize_email(membership.get("email")) or email_by_user_id.get(membership.get("user"), "")
        if not email:
            continue

        role_name = membership.get("role_name")
        role_info = None
        role_id = membership.get("role")
        if role_id in role_by_id:
            role_info = role_by_id[role_id]
        elif role_name:
            role_info = role_by_name.get(role_name.strip().lower())

        mapped_role = layer2.map_taiga_membership_role(
            role_name,
            bool(membership.get("is_admin", False)),
            (role_info or {}).get("permissions", []) or [],
        )
        created_at = membership.get("created_at")
        current = targets.get(email)
        if current is None:
            targets[email] = {
                "role": mapped_role,
                "created_at": created_at,
                "role_name": role_name,
                "is_admin": bool(membership.get("is_admin", False)),
            }
        else:
            current["role"] = max(int(current["role"]), int(mapped_role))
            if created_at and (not current.get("created_at") or created_at < current["created_at"]):
                current["created_at"] = created_at
            if bool(membership.get("is_admin", False)):
                current["is_admin"] = True
            if role_name and not current.get("role_name"):
                current["role_name"] = role_name

    for email, user in taiga_user_rows.items():
        targets.setdefault(
            email,
            {
                "role": layer2.PLANE_ROLE_MEMBER,
                "created_at": user.get("date_joined"),
                "role_name": None,
                "is_admin": False,
            },
        )
        if user.get("date_joined") and (
            not targets[email].get("created_at") or user.get("date_joined") < targets[email]["created_at"]
        ):
            targets[email]["created_at"] = user.get("date_joined")

    return taiga_user_rows, targets


def ensure_workspace_users(cursor, workspace_id, taiga_user_rows, workspace_targets, dry_run=False):
    template_user = layer2.get_template_user(cursor)
    template_ws_member = layer2.get_template_workspace_member(cursor)
    ws_view_props_json = layer2.to_json(template_ws_member["view_props"])
    ws_default_props_json = layer2.to_json(template_ws_member["default_props"])
    ws_issue_props_json = layer2.to_json(template_ws_member["issue_props"])

    created_users = 0
    created_workspace_members = 0
    updated_workspace_members = 0

    now = now_utc()

    for index, email in enumerate(sorted(taiga_user_rows.keys()), start=1):
        if index == 1 or index % 50 == 0 or index == len(taiga_user_rows):
            print(f"  -> Syncing Plane workspace users {index}/{len(taiga_user_rows)}...", flush=True)

        user = taiga_user_rows[email]
        target = workspace_targets.get(email, {})
        desired_role = int(target.get("role") or layer2.PLANE_ROLE_MEMBER)
        membership_created_at = target.get("created_at") or user.get("date_joined") or now
        user_created_at = user.get("date_joined") or membership_created_at or now

        existing_id = layer2.lookup_user_id(cursor, email)
        if existing_id:
            user_id = existing_id
        else:
            user_id = str(uuid4())
            username = (user.get("username") or uuid4().hex)[:255]
            display_name = (user.get("full_name") or user.get("full_name_display") or email.split("@")[0])[:255]
            if not dry_run:
                cursor.execute(
                    """
                    INSERT INTO users (
                        password, last_login, id, username, mobile_number, email, first_name, last_name, avatar,
                        date_joined, created_at, updated_at, last_location, created_location, is_superuser, is_managed,
                        is_password_expired, is_active, is_staff, is_email_verified, is_password_autoset, token,
                        user_timezone, last_active, last_login_time, last_logout_time, last_login_ip, last_logout_ip,
                        last_login_medium, last_login_uagent, token_updated_at, is_bot, cover_image, display_name,
                        avatar_asset_id, cover_image_asset_id, bot_type, is_email_valid, masked_at
                    ) VALUES (
                        %s, NULL, %s, %s, NULL, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, FALSE, FALSE,
                        FALSE, %s, FALSE, FALSE, TRUE, %s,
                        %s, NULL, NULL, NULL, %s, %s,
                        %s, %s, NULL, FALSE, NULL, %s,
                        NULL, NULL, NULL, %s, NULL
                    )
                    """,
                    (
                        template_user["password"],
                        user_id,
                        username,
                        email,
                        display_name,
                        "",
                        template_user["avatar"],
                        user_created_at,
                        user_created_at,
                        now,
                        template_user["last_location"],
                        template_user["created_location"],
                        bool(user.get("is_active", True)),
                        uuid4().hex * 2,
                        template_user["user_timezone"],
                        template_user["last_login_ip"],
                        template_user["last_logout_ip"],
                        template_user["last_login_medium"],
                        template_user["last_login_uagent"],
                        display_name,
                        template_user["is_email_valid"],
                    ),
                )
            cache_key = layer2.normalize_email(email)
            if cache_key:
                layer2.USER_ID_CACHE[cache_key] = user_id
            created_users += 1

        cursor.execute(
            """
            SELECT id, role, created_at FROM workspace_members
            WHERE workspace_id = %s AND member_id = %s AND deleted_at IS NULL
            """,
            (workspace_id, user_id),
        )
        ws_row = cursor.fetchone()
        if not ws_row:
            if not dry_run:
                cursor.execute(
                    """
                    INSERT INTO workspace_members (
                        created_at, updated_at, id, role, created_by_id, member_id, updated_by_id, workspace_id,
                        company_role, view_props, default_props, issue_props, is_active, deleted_at
                    ) VALUES (
                        %s, %s, %s, %s, NULL, %s, NULL, %s,
                        NULL, %s::jsonb, %s::jsonb, %s::jsonb, TRUE, NULL
                    )
                    """,
                    (
                        membership_created_at,
                        now,
                        str(uuid4()),
                        desired_role,
                        user_id,
                        workspace_id,
                        ws_view_props_json,
                        ws_default_props_json,
                        ws_issue_props_json,
                    ),
                )
            created_workspace_members += 1
        else:
            ws_id, existing_role, existing_created_at = ws_row
            target_role = max(int(existing_role), desired_role)
            should_update_created_at = (
                membership_created_at
                and existing_created_at
                and str(membership_created_at) < str(existing_created_at)
            )
            if int(existing_role) != target_role or should_update_created_at:
                if not dry_run:
                    if should_update_created_at:
                        cursor.execute(
                            "UPDATE workspace_members SET role = %s, created_at = %s, updated_at = %s WHERE id = %s",
                            (target_role, membership_created_at, now, ws_id),
                        )
                    else:
                        cursor.execute(
                            "UPDATE workspace_members SET role = %s, updated_at = %s WHERE id = %s",
                            (target_role, now, ws_id),
                        )
                updated_workspace_members += 1

    return {
        "created_users": created_users,
        "created_workspace_members": created_workspace_members,
        "updated_workspace_members": updated_workspace_members,
    }


def save_report(prefix: str, payload):
    report_json = Path(f"{prefix}_report.json")
    report_md = Path(f"{prefix}_report.md")
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(payload, indent=2, default=str))

    lines = [
        "# Taiga Global User Pre-Sync Report",
        "",
        "## Summary",
        "",
        f"- Taiga users with email: `{payload['summary']['taiga_users_with_email']}`",
        f"- Taiga memberships: `{payload['summary']['taiga_memberships_total']}`",
        f"- Taiga roles: `{payload['summary']['taiga_roles_total']}`",
        f"- Created Plane users: `{payload['summary']['created_users']}`",
        f"- Created workspace memberships: `{payload['summary']['created_workspace_members']}`",
        f"- Updated workspace memberships: `{payload['summary']['updated_workspace_members']}`",
        f"- Dry run: `{payload['summary']['dry_run']}`",
        "",
        "## Sample Users",
        "",
    ]
    sample_users = payload.get("sample_users", [])
    if sample_users:
        lines.extend([f"- `{email}`" for email in sample_users])
    else:
        lines.append("- None")
    report_md.write_text("\n".join(lines) + "\n")
    return report_json, report_md


def main():
    args = parse_args()
    prefix = args.report_prefix

    print("Fetching Taiga users...", flush=True)
    taiga_users = taiga_sync.taiga_paginated("/users")
    taiga_users = taiga_sync.hydrate_taiga_users(taiga_users)
    print("Fetching Taiga memberships...", flush=True)
    taiga_memberships = taiga_sync.taiga_paginated("/memberships")
    print("Fetching Taiga roles...", flush=True)
    taiga_roles = taiga_sync.taiga_paginated("/roles")

    taiga_user_rows, workspace_targets = strongest_workspace_targets(taiga_users, taiga_memberships, taiga_roles)
    print(f"Taiga users with email ready for Plane sync: {len(taiga_user_rows)}", flush=True)

    conn = layer2.get_db_connection()
    try:
        with conn.cursor() as cursor:
            workspace_id = get_workspace_id(cursor, PLANE_WORKSPACE_SLUG)
            if not workspace_id:
                raise RuntimeError(f"Workspace slug not found in Plane DB: {PLANE_WORKSPACE_SLUG}")
            stats = ensure_workspace_users(cursor, workspace_id, taiga_user_rows, workspace_targets, dry_run=args.dry_run)
        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    report = {
        "summary": {
            "taiga_users_with_email": len(taiga_user_rows),
            "taiga_memberships_total": len(taiga_memberships),
            "taiga_roles_total": len(taiga_roles),
            "created_users": stats["created_users"],
            "created_workspace_members": stats["created_workspace_members"],
            "updated_workspace_members": stats["updated_workspace_members"],
            "dry_run": bool(args.dry_run),
        },
        "sample_users": sorted(list(taiga_user_rows.keys()))[:25],
    }
    report_json, report_md = save_report(prefix, report)
    print(f"JSON report saved to: {report_json}", flush=True)
    print(f"Markdown report saved to: {report_md}", flush=True)


if __name__ == "__main__":
    main()
