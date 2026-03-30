#!/usr/bin/env python3
"""
Layer 1 helper: fetch Taiga users/memberships/roles and enrich migration_mapping.json.

This script is intentionally conservative:
- it reads Taiga API data
- it reads Plane workspace members
- it enriches an existing migration_mapping.json
- it reports missing Plane users/membership coverage

It does not overwrite existing Plane users.
It does not currently create Plane users directly via API.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"Missing required environment variable: {name}")
        sys.exit(1)
    return value


TAIGA_API_URL = env_required("TAIGA_API_URL").rstrip("/")
TAIGA_AUTH_TOKEN = env_required("TAIGA_AUTH_TOKEN")
PLANE_URL = env_required("PLANE_URL").rstrip("/")
PLANE_WORKSPACE_SLUG = env_required("PLANE_WORKSPACE_SLUG")
PLANE_API_KEY = env_required("PLANE_API_KEY")


def normalize_email(value):
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


def taiga_headers():
    return {
        "Authorization": f"Bearer {TAIGA_AUTH_TOKEN}",
        "Content-Type": "application/json",
    }


def plane_headers():
    return {
        "X-Api-Key": PLANE_API_KEY,
        "Content-Type": "application/json",
    }


def taiga_get(url, params=None, retries=5):
    last_resp = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=taiga_headers(), params=params, timeout=60)
            last_resp = resp
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempt, 10)
                time.sleep(wait)
                continue
            return resp
        except requests.RequestException:
            time.sleep(min(2 ** attempt, 10))
    return last_resp


def plane_get(url, params=None, retries=5):
    last_resp = None
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=plane_headers(), params=params, timeout=60)
            last_resp = resp
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempt, 10)
                time.sleep(wait)
                continue
            return resp
        except requests.RequestException:
            time.sleep(min(2 ** attempt, 10))
    return last_resp


def taiga_paginated(path: str):
    """
    Fetch Taiga endpoint with common pagination.
    Supports both list and paginated-object responses.
    """
    items = []
    page = 1
    while True:
        print(f"  -> Fetching Taiga {path} page {page}...", flush=True)
        resp = taiga_get(f"{TAIGA_API_URL}/{path.lstrip('/')}", params={"page": page})
        if resp is None or resp.status_code != 200:
            body = resp.text[:500] if resp is not None else "No response"
            raise RuntimeError(f"Taiga API failed for {path} page {page}: {body}")

        data = resp.json()
        if isinstance(data, list):
            items.extend(data)
            print(f"     page {page}: received {len(data)} rows (running total {len(items)})", flush=True)
            # Taiga can return paginated data as a plain list.
            # Keep fetching until an empty page appears or the page comes back short.
            if not data or len(data) < 30:
                break
            page += 1
            continue

        batch = data.get("results") or data.get("data") or []
        if not isinstance(batch, list):
            raise RuntimeError(f"Unexpected Taiga pagination shape for {path}: {type(data).__name__}")
        items.extend(batch)
        print(f"     page {page}: received {len(batch)} rows (running total {len(items)})", flush=True)

        # Stop if no next page signal remains.
        if data.get("next") in (None, "") and not data.get("next_page") and not data.get("has_next", False):
            break
        if not batch:
            break
        page += 1

    return items


def taiga_user_detail(user_id):
    resp = taiga_get(f"{TAIGA_API_URL}/users/{user_id}")
    if resp is None or resp.status_code != 200:
        body = resp.text[:500] if resp is not None else "No response"
        raise RuntimeError(f"Taiga user detail fetch failed for user {user_id}: {body}")
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected Taiga user detail shape for user {user_id}: {type(data).__name__}")
    return data


def hydrate_taiga_users(taiga_users):
    hydrated = []
    total = len(taiga_users)
    for index, user in enumerate(taiga_users, start=1):
        if not isinstance(user, dict):
            continue
        user_id = user.get("id")
        if user_id is None:
            continue
        if index == 1 or index % 25 == 0 or index == total:
            print(f"  -> Hydrating Taiga user details {index}/{total}...", flush=True)
        detail = taiga_user_detail(user_id)
        merged = dict(user)
        merged.update(detail)
        hydrated.append(merged)
    return hydrated


def plane_workspace_members():
    resp = plane_get(f"{PLANE_URL}/api/v1/workspaces/{PLANE_WORKSPACE_SLUG}/members/")
    if resp is None or resp.status_code != 200:
        body = resp.text[:500] if resp is not None else "No response"
        raise RuntimeError(f"Plane workspace members fetch failed: {body}")

    data = resp.json()
    if isinstance(data, list):
        rows = data
    else:
        rows = data.get("results", [])

    members = {}
    for row in rows:
        member = row.get("member", row)
        email = normalize_email(member.get("email"))
        if not email:
            continue
        members[email] = {
            "plane_user_id": member.get("id") or row.get("id"),
            "email": email,
            "username": member.get("username"),
            "display_name": member.get("display_name") or member.get("first_name"),
            "workspace_member": True,
            "workspace_role": row.get("role"),
            "workspace_member_id": row.get("id"),
        }
    return members


def load_mapping(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Mapping file not found: {path}")
    return json.loads(path.read_text())


def save_json(path: Path, payload):
    path.write_text(json.dumps(payload, indent=2, default=str))


def build_project_lookup(mapping):
    by_slug = {}
    by_plane_id = {}
    for slug, info in (mapping.get("projects") or {}).items():
        if not isinstance(info, dict):
            continue
        plane_id = info.get("plane_id")
        by_slug[slug] = plane_id
        if plane_id:
            by_plane_id[plane_id] = slug
    return by_slug, by_plane_id


def enrich_mapping(mapping, taiga_users, taiga_memberships, taiga_roles, plane_members):
    project_slug_to_plane_id, _plane_id_to_slug = build_project_lookup(mapping)

    mapping.setdefault("taiga_users_detail", {})
    mapping.setdefault("taiga_memberships_detail", [])
    mapping.setdefault("taiga_roles_detail", {})
    mapping.setdefault("plane_users_detail", {})
    mapping.setdefault("user_sync_meta", {})

    # Build users detail by email.
    taiga_user_email_by_id = {}
    for user in taiga_users:
        email = normalize_email(user.get("email"))
        if not email:
            continue
        if user.get("id") is not None:
            taiga_user_email_by_id[user.get("id")] = email
        mapping["taiga_users_detail"][email] = {
            "taiga_user_id": user.get("id"),
            "username": user.get("username"),
            "full_name": user.get("full_name") or user.get("full_name_display"),
            "email": email,
            "is_active": user.get("is_active", True),
        }

    # Build role detail.
    role_by_id = {}
    role_by_name = {}
    for role in taiga_roles:
        role_id = role.get("id")
        name = role.get("name")
        if role_id is not None:
            role_by_id[role_id] = role
        if name:
            role_by_name[name] = role

    # Existing membership rows for dedupe.
    existing_memberships = set()
    for row in mapping.get("taiga_memberships_detail", []):
        if not isinstance(row, dict):
            continue
        existing_memberships.add(
            (
                normalize_email(row.get("email")),
                row.get("plane_project_id"),
                row.get("created_at"),
                row.get("role_name"),
            )
        )

    # Existing legacy membership rows for dedupe and compatibility.
    legacy_memberships = mapping.setdefault("taiga_memberships", [])
    legacy_membership_keys = {
        (normalize_email(m.get("email") or m.get("user")), m.get("plane_project_id"), m.get("created_at"))
        for m in legacy_memberships
        if isinstance(m, dict)
    }

    for membership in taiga_memberships:
        email = normalize_email(membership.get("email"))
        if not email:
            email = taiga_user_email_by_id.get(membership.get("user"), "")
        if not email:
            continue

        taiga_project_slug = membership.get("project_slug")
        taiga_project_name = membership.get("project_name")
        plane_project_id = None

        if taiga_project_slug and taiga_project_slug in project_slug_to_plane_id:
            plane_project_id = project_slug_to_plane_id[taiga_project_slug]
        elif taiga_project_name:
            for slug, info in (mapping.get("projects") or {}).items():
                if isinstance(info, dict) and info.get("name") == taiga_project_name:
                    plane_project_id = info.get("plane_id")
                    taiga_project_slug = slug
                    break

        role_obj = None
        if membership.get("role") in role_by_id:
            role_obj = role_by_id[membership.get("role")]
        elif membership.get("role_name") in role_by_name:
            role_obj = role_by_name[membership.get("role_name")]

        enriched = {
            "email": email,
            "taiga_user_id": membership.get("user_id") or membership.get("user"),
            "plane_project_id": plane_project_id,
            "taiga_project_id": membership.get("project"),
            "taiga_project_slug": taiga_project_slug,
            "taiga_project_name": taiga_project_name,
            "role_id": membership.get("role"),
            "role_name": membership.get("role_name") or (role_obj or {}).get("name"),
            "is_admin": bool(membership.get("is_admin", False)),
            "is_owner": bool(membership.get("is_owner", False)),
            "created_at": membership.get("created_at"),
            "invited_by": membership.get("invited_by"),
        }

        membership_key = (
            email,
            plane_project_id,
            enriched["created_at"],
            enriched["role_name"],
        )
        if membership_key not in existing_memberships:
            mapping["taiga_memberships_detail"].append(enriched)
            existing_memberships.add(membership_key)

        legacy_key = (email, plane_project_id, membership.get("created_at"))
        if plane_project_id and legacy_key not in legacy_membership_keys:
            legacy_memberships.append(
                {
                    "plane_project_id": plane_project_id,
                    "email": email,
                    "role": enriched["role_name"],
                    "is_admin": enriched["is_admin"],
                    "created_at": membership.get("created_at"),
                    "invited_by": membership.get("invited_by"),
                }
            )
            legacy_membership_keys.add(legacy_key)

        if plane_project_id:
            role_bucket = mapping["taiga_roles_detail"].setdefault(plane_project_id, {})
            if role_obj and enriched["role_name"]:
                role_bucket[enriched["role_name"]] = {
                    "role_id": role_obj.get("id"),
                    "slug": role_obj.get("slug"),
                    "permissions": role_obj.get("permissions", []) or [],
                    "order": role_obj.get("order"),
                    "computable": bool(role_obj.get("computable", False)),
                }

    for email, info in plane_members.items():
        mapping["plane_users_detail"][email] = info

    mapping["user_sync_meta"] = {
        "taiga_users_fetched": len(taiga_users),
        "taiga_memberships_fetched": len(taiga_memberships),
        "taiga_roles_fetched": len(taiga_roles),
        "plane_workspace_members_seen": len(plane_members),
        "last_sync_source": "taiga_api",
    }

    return mapping


def build_report(mapping):
    taiga_users_detail = mapping.get("taiga_users_detail") or {}
    plane_users_detail = mapping.get("plane_users_detail") or {}
    taiga_memberships_detail = mapping.get("taiga_memberships_detail") or []

    taiga_emails = set(taiga_users_detail.keys())
    plane_emails = set(plane_users_detail.keys())
    missing_plane_users = sorted(taiga_emails - plane_emails)
    matched_users = sorted(taiga_emails & plane_emails)

    membership_coverage = {}
    for row in taiga_memberships_detail:
        if not isinstance(row, dict):
            continue
        email = normalize_email(row.get("email"))
        if not email:
            continue
        membership_coverage.setdefault(email, 0)
        membership_coverage[email] += 1

    report = {
        "summary": {
            "taiga_users_total": len(taiga_emails),
            "plane_workspace_users_total": len(plane_emails),
            "matched_users_total": len(matched_users),
            "missing_plane_users_total": len(missing_plane_users),
            "taiga_memberships_detail_total": len(taiga_memberships_detail),
        },
        "missing_plane_users": missing_plane_users,
        "sample_matched_users": matched_users[:25],
        "membership_coverage": membership_coverage,
    }
    return report


def render_report_md(report):
    summary = report["summary"]
    lines = [
        "# Taiga User Sync Report",
        "",
        "## Summary",
        "",
        f"- Taiga users total: `{summary['taiga_users_total']}`",
        f"- Plane workspace users total: `{summary['plane_workspace_users_total']}`",
        f"- Matched users total: `{summary['matched_users_total']}`",
        f"- Missing Plane users total: `{summary['missing_plane_users_total']}`",
        f"- Taiga memberships detail total: `{summary['taiga_memberships_detail_total']}`",
        "",
        "## Missing Plane Users",
        "",
    ]
    if report["missing_plane_users"]:
        lines.extend([f"- `{email}`" for email in report["missing_plane_users"]])
    else:
        lines.append("- None")
    lines.extend(["", "## Sample Matched Users", ""])
    if report["sample_matched_users"]:
        lines.extend([f"- `{email}`" for email in report["sample_matched_users"]])
    else:
        lines.append("- None")
    return "\n".join(lines) + "\n"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Fetch Taiga users/memberships/roles and enrich migration_mapping.json"
    )
    parser.add_argument(
        "mapping_path",
        nargs="?",
        default="migration_mapping.json",
        help="Path to existing migration_mapping.json",
    )
    parser.add_argument(
        "--output-mapping",
        default=None,
        help="Path for updated mapping output. Defaults to overwriting the input mapping path.",
    )
    parser.add_argument(
        "--report-prefix",
        default="taiga_user_sync",
        help="Prefix for JSON/Markdown report files in the current directory.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not overwrite the mapping file; still produce reports.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    mapping_path = Path(args.mapping_path).resolve()
    output_mapping = Path(args.output_mapping).resolve() if args.output_mapping else mapping_path
    report_json_path = Path.cwd() / f"{args.report_prefix}_report.json"
    report_md_path = Path.cwd() / f"{args.report_prefix}_report.md"

    mapping = load_mapping(mapping_path)

    print("Fetching Taiga users...")
    taiga_users = taiga_paginated("/users")
    taiga_users = hydrate_taiga_users(taiga_users)
    print("Fetching Taiga memberships...")
    taiga_memberships = taiga_paginated("/memberships")
    print("Fetching Taiga roles...")
    taiga_roles = taiga_paginated("/roles")
    print("Fetching Plane workspace members...")
    plane_members = plane_workspace_members()

    enriched = enrich_mapping(mapping, taiga_users, taiga_memberships, taiga_roles, plane_members)
    report = build_report(enriched)

    if not args.dry_run:
        save_json(output_mapping, enriched)
        print(f"Updated mapping saved to: {output_mapping}")
    else:
        print("Dry run enabled: mapping file not modified.")

    save_json(report_json_path, report)
    report_md_path.write_text(render_report_md(report))
    print(f"JSON report saved to: {report_json_path}")
    print(f"Markdown report saved to: {report_md_path}")

    summary = report["summary"]
    print("\nSummary")
    print(f"  Taiga users total: {summary['taiga_users_total']}")
    print(f"  Plane workspace users total: {summary['plane_workspace_users_total']}")
    print(f"  Matched users total: {summary['matched_users_total']}")
    print(f"  Missing Plane users total: {summary['missing_plane_users_total']}")

    if report["missing_plane_users"]:
        print("\nMissing Plane users:")
        for email in report["missing_plane_users"][:20]:
            print(f"  - {email}")
        if len(report["missing_plane_users"]) > 20:
            print(f"  ... and {len(report['missing_plane_users']) - 20} more")


if __name__ == "__main__":
    main()
