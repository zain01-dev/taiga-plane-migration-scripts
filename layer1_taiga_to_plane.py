#!/usr/bin/env python3
"""
Layer 1: Taiga JSON Dump → Plane.so API Migration Script
=========================================================
Reads a Taiga project JSON export and creates all entities in Plane.so
via the REST API. Generates a migration_mapping.json for Layer 2 DB overrides.

Usage:
    python taiga_to_plane.py <taiga_dump.json>
"""

import json
import os
import sys
import time
import re
import mimetypes
try:
    import markdown  # type: ignore
except Exception:  # pragma: no cover
    markdown = None
import base64
import gzip
import requests
from uuid import uuid4
from urllib.parse import urlparse, urlunparse

# ==========================================
# CONFIGURATION
# ==========================================


def env_bool(name, default=False):
    """Parse common boolean env var values."""
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_float(name, default):
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return float(default)
    try:
        return float(raw)
    except ValueError:
        return float(default)

PLANE_URL = os.environ.get("PLANE_URL", "http://localhost:8080").rstrip("/")  # No trailing slash
PLANE_UPLOAD_BASE_URL = os.environ.get("PLANE_UPLOAD_BASE_URL", PLANE_URL).rstrip("/")
PLANE_API_KEY = os.environ.get("PLANE_API_KEY", "").strip()
PLANE_WORKSPACE_SLUG = os.environ.get("PLANE_WORKSPACE_SLUG", "").strip()
MIGRATE_TASKS_AS_SUBISSUES = env_bool("MIGRATE_TASKS_AS_SUBISSUES", False)
TAIGA_BASE_URL = os.environ.get("TAIGA_BASE_URL", "https://projects.arbisoft.com").rstrip("/")

# Optional client-side pacing between API calls. Default is disabled.
RATE_LIMIT_DELAY = env_float("PLANE_RATE_LIMIT_DELAY", 0.0)

# ==========================================
# GLOBALS
# ==========================================

PLANE_HEADERS = {"X-Api-Key": PLANE_API_KEY, "Content-Type": "application/json"}

if not PLANE_URL or not PLANE_API_KEY or not PLANE_WORKSPACE_SLUG:
    print("❌ Missing configuration.")
    print("Set env vars: PLANE_URL, PLANE_API_KEY, PLANE_WORKSPACE_SLUG")
    sys.exit(1)

migration_mapping = {
    "projects": {},
    "issues": {},
    "comments": {},
    "labels": {},
    "states": {},
    "users": {},
    "modules": {},
    "cycles": {},
    "pages": {},
    "attachments": {},
    "taiga_emails": [],
    "taiga_roles": {},
    "taiga_memberships": [],
}

stats = {
    "projects": 0,
    "states": 0,
    "labels": 0,
    "issues": 0,
    "comments": 0,
    "attachments": 0,
    "attachments_deduped": 0,
    "attachments_failed": 0,
    "modules": 0,
    "module_links": 0,
    "project_members": 0,
    "cycles": 0,
    "cycle_links": 0,
    "pages": 0,
    "pages_prepared": 0,
    "relations_detected": 0,
    "errors": [],
}

CURRENT_TAIGA_SLUG = None

# ==========================================
# UTILITY FUNCTIONS
# ==========================================


def api_call(method, url, json_data=None, files=None, retries=3):
    """Make an API call to Plane.so with retry logic."""
    time.sleep(RATE_LIMIT_DELAY)
    headers = dict(PLANE_HEADERS)
    if files:
        # Don't send Content-Type header when uploading files
        headers.pop("Content-Type", None)

    for attempt in range(retries):
        try:
            resp = requests.request(
                method,
                f"{PLANE_URL}/api/v1/{url}",
                headers=headers,
                json=json_data if not files else None,
                data=json_data if files else None,
                files=files,
                timeout=60,
            )
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 5))
                print(f"    ⏳ Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code >= 500:
                print(f"    ⚠️  Server error {resp.status_code}, retrying...")
                time.sleep(2 ** attempt)
                continue
            return resp
        except requests.exceptions.RequestException as e:
            print(f"    ⚠️  Request failed: {e}, retrying...")
            time.sleep(2 ** attempt)
    return None


def load_taiga_dump(filepath):
    """Load a Taiga JSON dump file (plain or gzipped)."""
    if filepath.endswith(".gz"):
        with gzip.open(filepath, "rt", encoding="utf-8") as f:
            return json.load(f)
    else:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)


def map_taiga_priority(taiga_priority_name):
    """Map Taiga priority names to Plane priority values."""
    mapping = {
        "low": "low",
        "normal": "none",
        "high": "high",
        "critical": "urgent",
    }
    return mapping.get(taiga_priority_name.lower(), "none") if taiga_priority_name else "none"


def normalize_tag_name(value):
    if value is None:
        return ""
    return str(value).strip().lower()


def find_issue_mapping(plane_project_id, item_type, taiga_ref):
    """Find an issue mapping safely, even when multiple projects share the same Taiga ref."""
    issues = migration_mapping.get("issues", {})
    direct_key = f"{item_type}_{taiga_ref}"
    direct = issues.get(direct_key)
    if isinstance(direct, dict) and direct.get("plane_project_id") == plane_project_id:
        return direct

    for info in issues.values():
        if not isinstance(info, dict):
            continue
        if (
            info.get("plane_project_id") == plane_project_id
            and str(info.get("taiga_ref")) == str(taiga_ref)
            and str(info.get("taiga_type")) == str(item_type)
        ):
            return info
    return None


def extract_requestor(item):
    """Best-effort extraction of Taiga requestor/requester style fields."""
    if not isinstance(item, dict):
        return None

    for key in ["requestor", "requester", "requested_by", "requestedBy"]:
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()

    custom_vals = item.get("custom_attributes_values", {})
    if isinstance(custom_vals, dict):
        attrs = custom_vals.get("attributes_values", custom_vals)
        if isinstance(attrs, dict):
            for key, value in attrs.items():
                if not isinstance(key, str):
                    continue
                normalized = key.strip().lower()
                if normalized in {"requestor", "requester", "requested by", "requested_by"}:
                    if isinstance(value, str) and value.strip():
                        return value.strip()
    return None


def extract_custom_start_date(item):
    """Best-effort extraction of a Taiga custom start date."""
    if not isinstance(item, dict):
        return None
    custom_vals = item.get("custom_attributes_values", {})
    if not isinstance(custom_vals, dict):
        return None
    attrs = custom_vals.get("attributes_values", custom_vals)
    if not isinstance(attrs, dict):
        return None
    for key, value in attrs.items():
        if not isinstance(key, str):
            continue
        if key.strip().lower() == "start date" and value:
            return extract_date(value)
    return None


def build_legacy_block(item, item_type):
    """Build the HTML legacy data block for an issue description."""
    rows = []
    rows.append(f"<tr><td><b>Original Type</b></td><td>{item_type}</td></tr>")
    rows.append(f"<tr><td><b>Taiga Ref</b></td><td>#{item.get('ref', 'N/A')}</td></tr>")
    if TAIGA_BASE_URL and CURRENT_TAIGA_SLUG and item.get("ref"):
        link = None
        if item_type.lower() == "story":
            link = f"{TAIGA_BASE_URL}/project/{CURRENT_TAIGA_SLUG}/us/{item.get('ref')}"
        elif item_type.lower() == "task":
            link = f"{TAIGA_BASE_URL}/project/{CURRENT_TAIGA_SLUG}/task/{item.get('ref')}"
        elif item_type.lower() == "issue":
            link = f"{TAIGA_BASE_URL}/project/{CURRENT_TAIGA_SLUG}/issue/{item.get('ref')}"
        if link:
            rows.append(
                f"<tr><td><b>Taiga Link</b></td><td><a href=\"{link}\" target=\"_blank\" rel=\"noopener noreferrer\">{link}</a></td></tr>"
            )

    if item.get("owner"):
        rows.append(f"<tr><td><b>Original Creator</b></td><td>{item['owner']}</td></tr>")
    requestor = extract_requestor(item)
    if requestor:
        rows.append(f"<tr><td><b>Original Requestor</b></td><td>{requestor}</td></tr>")
    if item.get("created_date"):
        rows.append(f"<tr><td><b>Created</b></td><td>{item['created_date']}</td></tr>")
    if item.get("modified_date"):
        rows.append(f"<tr><td><b>Last Modified</b></td><td>{item['modified_date']}</td></tr>")
    if item.get("status"):
        rows.append(f"<tr><td><b>Original Status</b></td><td>{item['status']}</td></tr>")
    if item.get("priority"):
        rows.append(f"<tr><td><b>Priority</b></td><td>{item['priority']}</td></tr>")
    if item.get("severity"):
        rows.append(f"<tr><td><b>Severity</b></td><td>{item['severity']}</td></tr>")
    if item.get("type"):
        rows.append(f"<tr><td><b>Issue Type</b></td><td>{item['type']}</td></tr>")
    if item.get("milestone"):
        rows.append(f"<tr><td><b>Sprint/Milestone</b></td><td>{item['milestone']}</td></tr>")
    if item.get("assigned_to"):
        rows.append(f"<tr><td><b>Assigned To</b></td><td>{item['assigned_to']}</td></tr>")
    if item.get("tags"):
        tag_str = ", ".join([t[0] if isinstance(t, list) else str(t) for t in item["tags"]])
        rows.append(f"<tr><td><b>Tags</b></td><td>{tag_str}</td></tr>")
    if item.get("is_blocked"):
        rows.append(f"<tr><td><b>Blocked</b></td><td>Yes — {item.get('blocked_note', '')}</td></tr>")
    if item.get("due_date"):
        rows.append(f"<tr><td><b>Due Date</b></td><td>{item['due_date']}</td></tr>")
    if item.get("due_date_reason"):
        rows.append(f"<tr><td><b>Due Date Reason</b></td><td>{item['due_date_reason']}</td></tr>")
    if item.get("finished_date"):
        rows.append(f"<tr><td><b>Finished</b></td><td>{item['finished_date']}</td></tr>")
    if item.get("external_reference"):
        rows.append(f"<tr><td><b>External Ref</b></td><td>{item['external_reference']}</td></tr>")

    # Custom attributes
    custom_vals = item.get("custom_attributes_values", {})
    if custom_vals and isinstance(custom_vals, dict):
        attrs = custom_vals.get("attributes_values", custom_vals)
        if attrs:
            for k, v in attrs.items():
                if v:
                    rows.append(f"<tr><td><b>Custom: {k}</b></td><td>{v}</td></tr>")

    # Role points (user stories)
    if item.get("role_points"):
        rp_str = ", ".join(
            [f"{rp.get('role', '?')}: {rp.get('points', '?')}" for rp in item["role_points"]]
        )
        rows.append(f"<tr><td><b>Story Points</b></td><td>{rp_str}</td></tr>")

    table_rows = "\n".join(rows)
    return f"""
<hr/>
<h3>📦 Taiga Legacy Data</h3>
<table>
{table_rows}
</table>
"""


def render_description(text):
    """Render Taiga markdown as HTML, preserving formatting and links."""
    if not text:
        return ""
    markdown_extensions = [
        "extra",
        "nl2br",
        "sane_lists",
        "tables",
        "fenced_code",
    ]
    if markdown is not None:
        try:
            return markdown.markdown(str(text), extensions=markdown_extensions)
        except Exception:
            pass
    try:
        raise RuntimeError("markdown unavailable")
    except Exception:
        escaped = (
            str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        escaped = escaped.replace("\n", "<br/>")
        url_re = re.compile(r"(https?://[^\s<]+)")
        return url_re.sub(r'<a href="\1" target="_blank" rel="noopener noreferrer">\1</a>', escaped)


def extract_date(datetime_str):
    """Extract just the date portion (YYYY-MM-DD) from a datetime string."""
    if not datetime_str:
        return None
    match = re.match(r"(\d{4}-\d{2}-\d{2})", datetime_str)
    return match.group(1) if match else None


def extract_author_email(user_info):
    """Extract a likely email/username from Taiga user fields."""
    if isinstance(user_info, dict):
        return user_info.get("email", user_info.get("username", "")) or ""
    if isinstance(user_info, list) and user_info:
        return user_info[0] if isinstance(user_info[0], str) else ""
    if isinstance(user_info, str):
        return user_info
    return ""


def get_last_activity(item):
    """Return last activity timestamp/email from Taiga history if available."""
    history = item.get("history", []) or []
    last_entry = None
    for entry in history:
        if not isinstance(entry, dict):
            continue
        if entry.get("created_at"):
            if last_entry is None or entry.get("created_at") > last_entry.get("created_at"):
                last_entry = entry
    if last_entry:
        return {
            "last_activity_at": last_entry.get("created_at"),
            "last_activity_email": extract_author_email(last_entry.get("user", {})),
        }
    return {"last_activity_at": item.get("modified_date"), "last_activity_email": item.get("owner")}


def parse_story_points(role_points):
    """Parse Taiga role_points list into a float total when possible."""
    if not role_points or not isinstance(role_points, list):
        return None
    total = 0.0
    found = False
    for rp in role_points:
        if not isinstance(rp, dict):
            continue
        raw = str(rp.get("points", "")).strip()
        if not raw:
            continue
        try:
            if "/" in raw:
                num, den = raw.split("/", 1)
                val = float(num) / float(den)
            else:
                val = float(raw)
        except Exception:
            continue
        total += val
        found = True
    return total if found else None


def slug_to_title(slug):
    """Convert a slug-like string into a readable page title."""
    text = str(slug or "").strip()
    if not text:
        return "Untitled Page"
    text = re.sub(r"[-_]+", " ", text).strip()
    return text.title() if text else "Untitled Page"


def normalize_upload_url(upload_url):
    """
    Normalize upload URLs so presigned paths remain reachable from the current runner.
    If Plane returns an internal Docker hostname or the wrong scheme/port, route via
    PLANE_UPLOAD_BASE_URL.
    """
    try:
        parsed = urlparse(upload_url)
        upload_base = urlparse(PLANE_UPLOAD_BASE_URL)
    except Exception:
        return upload_url

    if not parsed.netloc:
        return upload_url

    internal_hosts = {
        "localhost",
        "127.0.0.1",
        "plane-nginx",
        "plane-app-proxy-1",
        "proxy",
        "nginx",
    }
    hostname = (parsed.hostname or "").strip().lower()

    if (hostname in internal_hosts or hostname.endswith("-nginx") or hostname.endswith("-proxy")) and upload_base.netloc:
        return urlunparse(
            (
                upload_base.scheme or parsed.scheme,
                upload_base.netloc,
                parsed.path,
                parsed.params,
                parsed.query,
                parsed.fragment,
            )
        )

    return upload_url


def relation_entries_count(taiga_dump):
    """Detect relation-like structures in a Taiga dump."""
    relation_keys = [
        "issue_relations",
        "userstory_relations",
        "epic_relations",
        "task_relations",
        "issue_links",
        "userstory_links",
    ]
    total = 0
    for key in relation_keys:
        entries = taiga_dump.get(key, [])
        if isinstance(entries, list):
            total += len(entries)
    return total


# ==========================================
# PLANE ENTITY CREATION FUNCTIONS
# ==========================================


def create_plane_project(taiga_project):
    """Create a project in Plane.so from Taiga data."""
    project_name = taiga_project.get("name", "Untitled Project")
    print(f"\n📁 Creating project: {project_name}")

    # Plane requires a unique project identifier (<= 12 chars) per workspace.
    slug = taiga_project.get("slug") or project_name
    identifier_base = re.sub(r"[^A-Za-z0-9]", "", str(slug)).upper()[:12]
    identifier_base = identifier_base or "TAIGA"
    if identifier_base[0].isdigit():
        identifier_base = ("P" + identifier_base)[:12]

    def next_identifier(attempt):
        if attempt == 0:
            return identifier_base
        suffix = uuid4().hex[:3].upper()
        return (identifier_base[: max(0, 12 - len(suffix))] + suffix)[:12]

    def next_name(attempt):
        if attempt == 0:
            return project_name
        suffix = uuid4().hex[:4].upper()
        name = f"{project_name} ({suffix})"
        return name[:255]

    payload = {
        "name": project_name,
        "identifier": identifier_base,
        "description": taiga_project.get("description", "") or "",
        "external_source": "taiga",
        "external_id": str(taiga_project.get("slug") or project_name),
        "module_view": True,
        "cycle_view": True,
        "page_view": True,
    }

    resp = None
    for attempt in range(3):
        payload["name"] = next_name(attempt)
        payload["identifier"] = next_identifier(attempt)
        resp = api_call("POST", f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/", payload)
        if resp is not None and resp.status_code in (200, 201):
            break
        # Retry only on identifier/name conflicts (common when re-running local tests).
        if resp is None:
            break
        if resp.status_code not in (400, 409):
            break
        try:
            body = resp.json()
        except Exception:
            body = None
        if not (isinstance(body, dict) and ("identifier" in body or "name" in body)):
            break

    if resp is not None and resp.status_code in (200, 201):
        project_data = resp.json()
        plane_project_id = project_data["id"]
        plane_identifier = project_data.get("identifier", "")
        created_name = project_data.get("name", payload.get("name", project_name))
        print(f"  ✅ Created project: {created_name} (ID: {plane_project_id}, Identifier: {plane_identifier})")
        migration_mapping["projects"][taiga_project.get("slug", project_name)] = {
            "plane_id": plane_project_id,
            "name": created_name,
        }
        stats["projects"] += 1
        return plane_project_id
    else:
        err = resp.text if resp is not None else "No response"
        print(f"  ❌ Failed to create project: {err}")
        stats["errors"].append(f"Project creation failed: {project_name} — {err}")
        return None


def create_plane_states(plane_project_id, taiga_project):
    """Create Plane states from Taiga status lists."""
    state_map = {}  # taiga_status_name -> plane_state_id

    # Collect all statuses from all entity types
    all_statuses = []
    for status_key in ["us_statuses", "task_statuses", "issue_statuses"]:
        for s in taiga_project.get(status_key, []):
            name = s.get("name", "Unknown")
            if name not in [st["name"] for st in all_statuses]:
                all_statuses.append(s)

    print(f"  📊 Creating {len(all_statuses)} states...")
    for status in all_statuses:
        name = status.get("name", "Unknown")
        color = status.get("color", "#858e98")

        # Map Taiga status concepts to Plane state groups
        is_closed = status.get("is_closed", False)
        if is_closed:
            group = "completed"
        elif name.lower() in ["new", "ready", "backlog", "to do", "ready for test"]:
            group = "backlog"
        elif name.lower() in ["in progress", "doing", "started", "ongoing"]:
            group = "started"
        elif name.lower() in ["done", "closed", "resolved", "finished"]:
            group = "completed"
        elif name.lower() in ["cancelled", "rejected", "won't fix", "duplicate"]:
            group = "cancelled"
        else:
            group = "unstarted"

        payload = {
            "name": name,
            "color": color,
            "group": group,
        }

        resp = api_call(
            "POST",
            f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/states/",
            payload,
        )
        if resp is not None and resp.status_code in (200, 201):
            plane_state_id = resp.json()["id"]
            state_map[name] = plane_state_id
            stats["states"] += 1
        elif resp is not None and resp.status_code == 409:
            # State already exists, try to find it
            list_resp = api_call(
                "GET",
                f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/states/",
            )
            if list_resp is not None and list_resp.status_code == 200:
                for existing_state in list_resp.json().get("results", list_resp.json() if isinstance(list_resp.json(), list) else []):
                    if existing_state.get("name") == name:
                        state_map[name] = existing_state["id"]
                        break
        else:
            err = resp.text if resp is not None else "No response"
            print(f"    ⚠️  Failed to create state '{name}': {err}")

    migration_mapping["states"][plane_project_id] = state_map
    print(f"    Created {len(state_map)} states")
    return state_map


def create_plane_labels(plane_project_id, taiga_project):
    """Create Plane labels from Taiga tags."""
    label_map = {}  # tag_name -> plane_label_id

    tags_colors = taiga_project.get("tags_colors", [])
    if not tags_colors:
        return label_map

    # tags_colors is a list of [name, color] pairs
    print(f"  🏷️  Creating {len(tags_colors)} labels...")
    for tag_pair in tags_colors:
        if isinstance(tag_pair, list) and len(tag_pair) >= 2:
            tag_name, tag_color = tag_pair[0], tag_pair[1]
        elif isinstance(tag_pair, str):
            tag_name, tag_color = tag_pair, "#858e98"
        else:
            continue

        if not tag_color:
            tag_color = "#858e98"

        payload = {
            "name": tag_name,
            "color": tag_color,
        }

        resp = api_call(
            "POST",
            f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/labels/",
            payload,
        )
        normalized_tag_name = normalize_tag_name(tag_name)

        if resp is not None and resp.status_code in (200, 201):
            label_id = resp.json()["id"]
            label_map[tag_name] = label_id
            if normalized_tag_name:
                label_map[normalized_tag_name] = label_id
            stats["labels"] += 1
        else:
            err = resp.text if resp is not None else "No response"
            existing = api_call(
                "GET",
                f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/labels/",
            )
            matched_existing = None
            if existing is not None and existing.status_code == 200:
                existing_rows = existing.json()
                if isinstance(existing_rows, dict):
                    existing_rows = existing_rows.get("results", [])
                for row in existing_rows or []:
                    if normalize_tag_name(row.get("name")) == normalized_tag_name:
                        matched_existing = row
                        break
            if matched_existing:
                label_id = matched_existing["id"]
                label_map[tag_name] = label_id
                if normalized_tag_name:
                    label_map[normalized_tag_name] = label_id
            else:
                # Try to continue even if label creation fails
                print(f"    ⚠️  Failed to create label '{tag_name}': {err}")

    migration_mapping["labels"][plane_project_id] = label_map
    print(f"    Created {len(label_map)} labels")
    return label_map


def get_plane_users():
    """Fetch all workspace members' email -> id mapping."""
    user_map = {}
    resp = api_call(
        "GET",
        f"workspaces/{PLANE_WORKSPACE_SLUG}/members/",
    )
    if resp is not None and resp.status_code == 200:
        members = resp.json()
        if isinstance(members, list):
            for m in members:
                member = m.get("member", m)
                email = member.get("email", "")
                uid = member.get("id", m.get("id", ""))
                if email and uid:
                    user_map[email] = uid
        elif isinstance(members, dict):
            for m in members.get("results", []):
                member = m.get("member", m)
                email = member.get("email", "")
                uid = member.get("id", m.get("id", ""))
                if email and uid:
                    user_map[email] = uid
    print(f"  👥 Found {len(user_map)} Plane workspace members")
    migration_mapping["users"] = user_map
    return user_map


def map_taiga_member_role_to_plane(role_name, is_admin=False):
    """Best-effort Taiga membership role -> Plane project role."""
    if is_admin:
        return 20
    normalized = (role_name or "").strip().lower()
    if any(token in normalized for token in ("guest", "viewer", "read only", "read-only", "stakeholder")):
        return 5
    if any(token in normalized for token in ("owner", "admin", "manager", "lead", "product owner", "product-owner")):
        return 20
    return 15


def collect_project_user_targets(taiga_dump, user_map):
    """Collect project users that should exist before issue creation."""
    role_by_email = {}

    def note(email, role_name=None, is_admin=False, force_member=False):
        if not isinstance(email, str) or email not in user_map:
            return
        desired_role = map_taiga_member_role_to_plane(role_name, is_admin)
        if force_member and desired_role < 15:
            desired_role = 15
        current = role_by_email.get(email)
        role_by_email[email] = max(current or desired_role, desired_role)

    for membership in taiga_dump.get("memberships", []):
        if not isinstance(membership, dict):
            continue
        email = membership.get("email") or membership.get("user")
        note(email, membership.get("role"), bool(membership.get("is_admin", False)))

    for collection_key in ["user_stories", "tasks", "issues"]:
        for item in taiga_dump.get(collection_key, []):
            note(item.get("owner"), "member")
            note(item.get("assigned_to"), "member", force_member=True)
            for watcher in item.get("watchers", []) or []:
                note(watcher, "member", force_member=True)
            for attachment in item.get("attachments", []):
                note(attachment.get("owner"), "member")
            for entry in item.get("history", []):
                note(extract_author_email(entry.get("user", {})), "member")

    for epic in taiga_dump.get("epics", []):
        note(epic.get("owner"), "member")

    for milestone in taiga_dump.get("milestones", []):
        note(milestone.get("owner"), "member")

    for page in taiga_dump.get("wiki_pages", []):
        note(extract_author_email(page.get("owner")), "member")
        note(extract_author_email(page.get("last_modifier")), "member")

    return role_by_email


def ensure_project_members(plane_project_id, taiga_dump, user_map):
    """Ensure relevant users are project members before issue creation so assignees can stick."""
    project_members = {}
    resp = api_call("GET", f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/project-members/")
    if resp is not None and resp.status_code == 200:
        rows = resp.json()
        if isinstance(rows, dict):
            rows = rows.get("results", [])
        for row in rows or []:
            member = row.get("member", row)
            email = (member.get("email") or "").strip().lower()
            if email:
                project_members[email] = row

    targets = collect_project_user_targets(taiga_dump, user_map)
    if not targets:
        return project_members

    print(f"  👤 Ensuring {len(targets)} project members...")
    created = 0
    updated = 0
    for email, desired_role in sorted(targets.items()):
        existing = project_members.get(email)
        if existing:
            existing_role = int(existing.get("role") or 0)
            if existing_role < desired_role:
                member_id = existing.get("id")
                resp = api_call(
                    "PATCH",
                    f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/project-members/{member_id}/",
                    {"role": desired_role},
                )
                if resp is not None and resp.status_code in (200, 201):
                    existing["role"] = desired_role
                    updated += 1
            continue

        resp = api_call(
            "POST",
            f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/project-members/",
            {"member": user_map[email], "role": desired_role},
        )
        if resp is not None and resp.status_code in (200, 201):
            row = resp.json()
            member = row.get("member", {})
            project_members[email] = row if isinstance(row, dict) else {"member": {"email": email}}
            if not member and email:
                project_members[email]["member"] = {"email": email}
            created += 1
            stats["project_members"] += 1
        else:
            err = resp.text if resp is not None else "No response"
            stats["errors"].append(f"Project member create failed ({email}): {err}")

    print(f"    Project members created: {created}, updated: {updated}")
    return project_members


def collect_taiga_emails(taiga_dump):
    """Collect all email-like identifiers from the Taiga dump for user creation."""
    emails = set(migration_mapping.get("taiga_emails", []))
    for item in taiga_dump.get("user_stories", []) + taiga_dump.get("tasks", []) + taiga_dump.get("issues", []):
        for key in ["owner", "assigned_to", "assigned_to_email"]:
            val = item.get(key)
            if isinstance(val, str) and "@" in val:
                emails.add(val)
        for watcher in item.get("watchers", []) or []:
            if isinstance(watcher, str) and "@" in watcher:
                emails.add(watcher)
        for entry in item.get("history", []):
            author = extract_author_email(entry.get("user", {}))
            if author and "@" in author:
                emails.add(author)
        for attachment in item.get("attachments", []):
            owner = attachment.get("owner")
            if isinstance(owner, str) and "@" in owner:
                emails.add(owner)

    for epic in taiga_dump.get("epics", []):
        owner = epic.get("owner")
        if isinstance(owner, str) and "@" in owner:
            emails.add(owner)

    for milestone in taiga_dump.get("milestones", []):
        owner = milestone.get("owner")
        if isinstance(owner, str) and "@" in owner:
            emails.add(owner)

    for page in taiga_dump.get("wiki_pages", []):
        for key in ["owner", "last_modifier"]:
            val = extract_author_email(page.get(key))
            if val and "@" in val:
                emails.add(val)

    for membership in taiga_dump.get("memberships", []):
        if not isinstance(membership, dict):
            continue
        for key in ["user", "email", "invited_by"]:
            val = membership.get(key)
            if isinstance(val, str) and "@" in val:
                emails.add(val)

    migration_mapping["taiga_emails"] = sorted(emails)


def collect_taiga_memberships(taiga_dump, plane_project_id):
    """Collect Taiga roles/memberships for Layer 2 role-aware member creation."""
    role_map = {}
    for role in taiga_dump.get("roles", []):
        if not isinstance(role, dict):
            continue
        role_name = (role.get("name") or "").strip()
        if not role_name:
            continue
        role_map[role_name] = {
            "slug": role.get("slug"),
            "permissions": role.get("permissions", []) or [],
            "computable": bool(role.get("computable", False)),
            "order": role.get("order"),
        }
    migration_mapping["taiga_roles"][plane_project_id] = role_map

    for membership in taiga_dump.get("memberships", []):
        if not isinstance(membership, dict):
            continue
        email = membership.get("email") or membership.get("user")
        if not isinstance(email, str) or "@" not in email:
            continue
        migration_mapping["taiga_memberships"].append(
            {
                "plane_project_id": plane_project_id,
                "email": email,
                "role": membership.get("role"),
                "is_admin": bool(membership.get("is_admin", False)),
                "created_at": membership.get("created_at"),
                "invited_by": membership.get("invited_by"),
            }
        )


def create_plane_issue(
    plane_project_id,
    item,
    item_type,
    state_map,
    label_map,
    user_map,
    parent_issue_id=None,
    parent_story_ref=None,
):
    """Create a single Plane issue from a Taiga user story / task / issue."""
    subject = item.get("subject", "Untitled")
    taiga_ref = item.get("ref", "?")

    # Build the name with type prefix
    name = f"[{item_type}] {subject}"
    if len(name) > 255:
        name = name[:252] + "..."

    # Build description with legacy block
    description = item.get("description", "") or ""
    legacy_block = build_legacy_block(item, item_type)
    if description:
        safe_desc = render_description(description)
        full_description_html = f"{safe_desc}{legacy_block}"
    else:
        full_description_html = legacy_block

    # Map priority
    priority = "none"
    if item.get("priority"):
        priority = map_taiga_priority(item["priority"])

    # Map state
    state_id = None
    status_name = item.get("status")
    if status_name and status_name in state_map:
        state_id = state_map[status_name]

    # Map labels from tags
    label_ids = []
    if item.get("tags"):
        for tag in item["tags"]:
            tag_name = tag[0] if isinstance(tag, list) else str(tag)
            label_id = label_map.get(tag_name) or label_map.get(normalize_tag_name(tag_name))
            if label_id:
                label_ids.append(label_id)

    # Map assignee
    assignee_ids = []
    if item.get("assigned_to") and item["assigned_to"] in user_map:
        assignee_ids.append(user_map[item["assigned_to"]])

    # Map dates
    start_date = extract_custom_start_date(item) or extract_date(item.get("created_date"))
    target_date = extract_date(item.get("due_date"))
    if start_date and target_date and start_date > target_date:
        target_date = start_date
    created_at = item.get("created_date")
    author_email = item.get("owner")
    taiga_issue_type = item.get("type")
    story_points = None
    if item_type.lower() == "story":
        story_points = parse_story_points(item.get("role_points"))
    last_activity = get_last_activity(item)

    payload = {
        "name": name,
        "description_html": full_description_html,
        "priority": priority,
        "external_source": "taiga",
        "external_id": f"{item_type.lower()}-{taiga_ref}",
    }

    if state_id:
        payload["state"] = state_id
    if label_ids:
        payload["labels"] = sorted(set(label_ids))
    if assignee_ids:
        payload["assignees"] = assignee_ids
    if start_date:
        payload["start_date"] = start_date
    if created_at:
        payload["created_at"] = created_at
    if author_email and author_email in user_map:
        payload["created_by"] = user_map[author_email]
    if target_date:
        payload["target_date"] = target_date
    if parent_issue_id:
        payload["parent"] = parent_issue_id

    resp = api_call(
        "POST",
        f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/issues/",
        payload,
    )

    if (
        parent_issue_id
        and resp is not None
        and resp.status_code not in (200, 201)
        and "parent" in (resp.text or "").lower()
    ):
        # Fallback for API variants where `parent` is rejected.
        fallback_payload = dict(payload)
        fallback_payload.pop("parent", None)
        resp = api_call(
            "POST",
            f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/issues/",
            fallback_payload,
        )
        if resp is not None and resp.status_code in (200, 201):
            stats["errors"].append(
                f"Task #{taiga_ref}: parent link rejected by API, created as top-level issue"
            )
            parent_issue_id = None

    if resp is not None and resp.status_code in (200, 201):
        plane_issue = resp.json()
        plane_issue_id = plane_issue["id"]
        print(f"    ✅ Issue #{taiga_ref}: {subject[:50]}")

        # Store mapping with metadata for Layer 2
        migration_mapping["issues"][f"{item_type}_{taiga_ref}"] = {
            "plane_id": plane_issue_id,
            "plane_project_id": plane_project_id,
            "taiga_ref": taiga_ref,
            "taiga_type": item_type,
            "created_at": item.get("created_date"),
            "modified_at": item.get("modified_date"),
            "author_email": item.get("owner"),
            "assigned_to_email": item.get("assigned_to"),
            "watchers_emails": [
                watcher
                for watcher in (item.get("watchers", []) or [])
                if isinstance(watcher, str) and "@" in watcher
            ],
            "parent_story_ref": parent_story_ref,
            "parent_issue_id": parent_issue_id,
            "taiga_issue_type": taiga_issue_type,
            "story_points": story_points,
            "last_activity_at": last_activity.get("last_activity_at"),
            "last_activity_email": last_activity.get("last_activity_email"),
        }
        stats["issues"] += 1
        return plane_issue_id
    else:
        err = resp.text if resp is not None else "No response"
        print(f"    ❌ Issue #{taiga_ref} failed: {err}")
        stats["errors"].append(f"Issue #{taiga_ref} ({item_type}): {err}")
        return None


def create_plane_comments(plane_project_id, plane_issue_id, item, user_map, item_type):
    """Create Plane comments from Taiga history entries."""
    history = item.get("history", [])
    taiga_ref = item.get("ref", "?")
    taiga_type = item_type or "Item"

    for idx, entry in enumerate(history):
        comment_text = entry.get("comment", "")
        if not comment_text:
            continue

        # Build comment with original author attribution
        author_email = extract_author_email(entry.get("user", {})) or "Unknown"

        created_at = entry.get("created_at", "")
        synthetic_id = f"{taiga_type}_{taiga_ref}_{created_at}_{idx}"

        attributed_comment = (
            f"<p><em>Originally by <strong>{author_email}</strong> "
            f"on {created_at}</em></p>"
            f"<p>{comment_text}</p>"
        )

        payload = {
            "comment_html": attributed_comment,
            "external_source": "taiga",
            "external_id": synthetic_id,
        }

        if created_at:
            payload["created_at"] = created_at

        # Set actor/creator if user exists in Plane
        if author_email in user_map:
            payload["created_by"] = user_map[author_email]

        resp = api_call(
            "POST",
            f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/issues/{plane_issue_id}/comments/",
            payload,
        )

        if resp is not None and resp.status_code in (200, 201):
            plane_comment = resp.json()
            plane_comment_id = plane_comment.get("id", "")
            migration_mapping["comments"][synthetic_id] = {
                "plane_id": plane_comment_id,
                "plane_issue_id": plane_issue_id,
                "plane_project_id": plane_project_id,
                "created_at": created_at,
                "author_email": author_email if author_email != "Unknown" else "",
            }
            stats["comments"] += 1
        else:
            err = resp.text if resp is not None else "No response"
            print(f"      ⚠️  Comment on #{taiga_ref} failed: {err}")
            stats["errors"].append(f"Comment on #{taiga_ref}: {err}")


def upload_attachments(plane_project_id, plane_issue_id, item, item_type=None):
    """Upload attachments from Taiga to Plane."""
    attachments = item.get("attachments", [])
    taiga_ref = item.get("ref", "?")
    seen_attachment_keys = set()

    for attachment in attachments:
        attached_file = attachment.get("attached_file")
        if not attached_file:
            continue

        filename = attachment.get("name") or None
        file_content = None

        # Taiga exports can embed attachments as base64 in `attached_file`.
        # They can also contain a URL string (depending on exporter/config).
        if isinstance(attached_file, dict):
            if not filename:
                filename = attached_file.get("name") or "attachment"
            data = attached_file.get("data")
            if not data:
                print(f"      ⚠️  Attachment missing data: {filename}")
                stats["attachments_failed"] += 1
                continue
            try:
                file_content = base64.b64decode(data)
            except Exception as e:
                print(f"      ⚠️  Attachment base64 decode failed ({filename}): {e}")
                stats["attachments_failed"] += 1
                continue
        elif isinstance(attached_file, str):
            if not filename:
                filename = os.path.basename(attached_file) or "attachment"
            if attached_file.startswith("http"):
                try:
                    file_resp = requests.get(attached_file, timeout=30)
                    if file_resp.status_code != 200:
                        print(f"      ⚠️  Could not download attachment: {filename}")
                        stats["attachments_failed"] += 1
                        continue
                    file_content = file_resp.content
                except Exception as e:
                    print(f"      ⚠️  Attachment download error ({filename}): {e}")
                    stats["attachments_failed"] += 1
                    continue
            else:
                print(f"      ⚠️  Skipping unsupported attachment reference: {filename}")
                stats["attachments_failed"] += 1
                continue
        else:
            print(f"      ⚠️  Skipping unsupported attachment type: {type(attached_file).__name__}")
            stats["attachments_failed"] += 1
            continue

        content_type = attachment.get("content_type") or mimetypes.guess_type(filename)[0]
        if not content_type:
            try:
                file_content.decode("utf-8")
                content_type = "text/plain"
            except Exception:
                content_type = "application/octet-stream"

        dedupe_key = (
            str(attachment.get("sha1")) if attachment.get("sha1") else f"{filename}:{len(file_content)}"
        )
        if dedupe_key in seen_attachment_keys:
            print(f"      📎 Duplicate attachment skipped: {filename}")
            stats["attachments_deduped"] += 1
            continue
        seen_attachment_keys.add(dedupe_key)

        # Plane uses a presigned upload flow for issue attachments:
        # 1) create attachment record (returns presigned POST fields)
        # 2) upload file to storage using the presigned POST
        meta_payload = {
            "name": filename,
            "type": content_type,
            "size": len(file_content),
            "external_source": "taiga",
        }
        if attachment.get("sha1"):
            meta_payload["external_id"] = str(attachment.get("sha1"))

        meta_resp = api_call(
            "POST",
            f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/issues/{plane_issue_id}/issue-attachments/",
            json_data=meta_payload,
        )

        if meta_resp is not None and meta_resp.status_code == 409:
            print(f"      📎 Attachment already exists: {filename}")
            stats["attachments_deduped"] += 1
            continue

        if meta_resp is None or meta_resp.status_code not in (200, 201):
            err = meta_resp.text if meta_resp is not None else "No response"
            print(f"      ⚠️  Attachment metadata failed ({filename}): {err}")
            stats["errors"].append(f"Attachment metadata failed: #{taiga_ref} {filename} — {err}")
            stats["attachments_failed"] += 1
            continue

        try:
            meta_json = meta_resp.json()
            upload_data = meta_json.get("upload_data") or {}
            upload_url = upload_data.get("url")
            upload_fields = upload_data.get("fields") or {}
            asset_id = meta_json.get("asset_id") or (meta_json.get("attachment") or {}).get("id")
        except Exception as e:
            print(f"      ⚠️  Attachment metadata parse failed ({filename}): {e}")
            stats["errors"].append(f"Attachment metadata parse failed: #{taiga_ref} {filename} — {e}")
            stats["attachments_failed"] += 1
            continue

        if not upload_url or not upload_fields:
            print(f"      ⚠️  Missing presigned upload data ({filename})")
            stats["errors"].append(f"Attachment presign missing: #{taiga_ref} {filename}")
            stats["attachments_failed"] += 1
            continue

        upload_url = normalize_upload_url(upload_url)

        try:
            upload_resp = requests.post(
                upload_url,
                data=upload_fields,
                files={"file": (filename, file_content, content_type)},
                timeout=60,
            )
        except Exception as e:
            print(f"      ⚠️  Attachment upload request failed ({filename}): {e}")
            stats["errors"].append(f"Attachment upload request failed: #{taiga_ref} {filename} — {e}")
            stats["attachments_failed"] += 1
            continue

        if upload_resp.status_code in (200, 201, 204):
            print(f"      📎 Uploaded: {filename}")
            stats["attachments"] += 1
            if asset_id:
                map_key = f"{item_type or 'Item'}_{taiga_ref}_{attachment.get('sha1') or filename}_{len(migration_mapping['attachments'])}"
                migration_mapping["attachments"][map_key] = {
                    "asset_id": asset_id,
                    "plane_issue_id": plane_issue_id,
                    "plane_project_id": plane_project_id,
                    "taiga_ref": taiga_ref,
                    "taiga_type": item_type or "Item",
                    "filename": filename,
                    "created_at": attachment.get("created_date"),
                    "modified_at": attachment.get("modified_date"),
                    "author_email": attachment.get("owner"),
                    "size": attachment.get("size"),
                }
            if asset_id:
                confirm_resp = api_call(
                    "PATCH",
                    f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/issues/{plane_issue_id}/issue-attachments/{asset_id}/",
                    json_data={"is_uploaded": True},
                )
                if confirm_resp is None or confirm_resp.status_code not in (200, 204):
                    err = confirm_resp.text if confirm_resp is not None else "No response"
                    stats["errors"].append(
                        f"Attachment confirm failed: #{taiga_ref} {filename} — {err}"
                    )
        else:
            print(f"      ⚠️  Attachment upload failed ({filename}): HTTP {upload_resp.status_code}")
            stats["errors"].append(
                f"Attachment upload failed: #{taiga_ref} {filename} — HTTP {upload_resp.status_code}"
            )
            stats["attachments_failed"] += 1


def create_plane_modules(plane_project_id, taiga_project, user_map):
    """Create Plane modules from Taiga epics."""
    epics = taiga_project.get("epics", [])
    if not epics:
        return {}

    module_map = {}  # taiga_epic_ref -> plane_module_id

    print(f"  📦 Creating {len(epics)} modules from epics...")
    for epic in epics:
        name = epic.get("subject", "Untitled Epic")
        description = epic.get("description", "") or ""
        ref = epic.get("ref", "?")

        payload = {
            "name": f"[Epic #{ref}] {name}",
            "description": description,
            "external_source": "taiga",
            "external_id": f"epic-{ref}",
        }

        if epic.get("created_date"):
            start_date = extract_date(epic["created_date"])
            if start_date:
                payload["start_date"] = start_date

        resp = api_call(
            "POST",
            f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/modules/",
            payload,
        )

        if resp is not None and resp.status_code in (200, 201):
            module_data = resp.json()
            module_id = module_data["id"]
            module_map[ref] = module_id
            related_us_refs = []
            for rel in epic.get("related_user_stories", []):
                if isinstance(rel, dict) and rel.get("user_story") is not None:
                    related_us_refs.append(rel.get("user_story"))
            migration_mapping["modules"][f"epic_{ref}"] = {
                "plane_id": module_id,
                "plane_project_id": plane_project_id,
                "created_at": epic.get("created_date"),
                "author_email": epic.get("owner"),
                "taiga_epic_ref": ref,
                "related_story_refs": related_us_refs,
            }
            stats["modules"] += 1
            print(f"    ✅ Module (Epic #{ref}): {name[:50]}")
        else:
            err = resp.text if resp is not None else "No response"
            print(f"    ❌ Module (Epic #{ref}) failed: {err}")
            stats["errors"].append(f"Module (Epic #{ref}) failed: {err}")

    return module_map


def link_modules_to_issues(plane_project_id):
    """Link previously created story issues to their epic modules."""
    modules = migration_mapping.get("modules", {})
    if not modules:
        return

    linked = 0
    print("  🔗 Linking module issues from epic relationships...")
    for module_key, info in modules.items():
        module_id = info.get("plane_id")
        story_refs = info.get("related_story_refs", [])
        if not module_id or not story_refs:
            continue

        plane_issue_ids = []
        for story_ref in story_refs:
            mapped = find_issue_mapping(plane_project_id, "Story", story_ref)
            if mapped and mapped.get("plane_id"):
                plane_issue_ids.append(mapped["plane_id"])

        if not plane_issue_ids:
            continue

        resp = api_call(
            "POST",
            f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/modules/{module_id}/module-issues/",
            json_data={"issues": sorted(set(plane_issue_ids))},
        )
        if resp is not None and resp.status_code in (200, 201):
            link_count = len(set(plane_issue_ids))
            linked += link_count
            stats["module_links"] += link_count
        else:
            err = resp.text if resp is not None else "No response"
            stats["errors"].append(f"Module link failed ({module_key}): {err}")

    print(f"    Linked {linked} module-issue relations")


def create_plane_cycles(plane_project_id, taiga_project):
    """Create Plane cycles from Taiga milestones."""
    milestones = taiga_project.get("milestones", [])
    if not milestones:
        return {}

    cycle_map = {}  # milestone name -> cycle_id
    print(f"  🗓️  Creating {len(milestones)} cycles from milestones...")

    for milestone in milestones:
        name = milestone.get("name")
        if not name:
            continue
        payload = {
            "name": name,
            "description": "",
            "external_source": "taiga",
            "external_id": str(milestone.get("slug") or name),
            "project_id": plane_project_id,
        }

        resp = api_call(
            "POST",
            f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/cycles/",
            json_data=payload,
        )

        if resp is not None and resp.status_code in (200, 201):
            cycle_id = resp.json().get("id")
            cycle_map[name] = cycle_id
            migration_mapping["cycles"][f"{plane_project_id}:{name}"] = {
                "plane_id": cycle_id,
                "plane_project_id": plane_project_id,
                "created_at": milestone.get("created_date"),
                "author_email": milestone.get("owner"),
                "start_date": milestone.get("estimated_start"),
                "end_date": milestone.get("estimated_finish"),
            }
            stats["cycles"] += 1
            continue

        if resp is not None and resp.status_code == 409:
            body = {}
            try:
                body = resp.json()
            except Exception:
                body = {}
            existing_id = body.get("id")
            if existing_id:
                cycle_map[name] = existing_id
                continue

        err = resp.text if resp is not None else "No response"
        stats["errors"].append(f"Cycle creation failed ({name}): {err}")

    print(f"    Created {len(cycle_map)} cycles")
    return cycle_map


def link_cycles_to_issues(plane_project_id, taiga_dump, cycle_map):
    """Link migrated story/task/issue items to cycles based on milestone names."""
    if not cycle_map:
        return

    cycle_issue_map = {}
    for item_type, collection_key in [("Story", "user_stories"), ("Task", "tasks"), ("Issue", "issues")]:
        for item in taiga_dump.get(collection_key, []):
            milestone_name = item.get("milestone")
            if not milestone_name:
                continue
            cycle_id = cycle_map.get(milestone_name)
            if not cycle_id:
                continue
            issue_key = f"{item_type}_{item.get('ref')}"
            issue_info = find_issue_mapping(plane_project_id, item_type, item.get("ref")) or {}
            plane_issue_id = issue_info.get("plane_id")
            if not plane_issue_id:
                continue
            cycle_issue_map.setdefault(cycle_id, set()).add(plane_issue_id)

    if not cycle_issue_map:
        return

    print("  🔗 Linking cycle issues from milestone memberships...")
    for cycle_id, issue_ids in cycle_issue_map.items():
        resp = api_call(
            "POST",
            f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/cycles/{cycle_id}/cycle-issues/",
            json_data={"issues": sorted(issue_ids)},
        )
        if resp is not None and resp.status_code in (200, 201):
            stats["cycle_links"] += len(issue_ids)
        else:
            err = resp.text if resp is not None else "No response"
            stats["errors"].append(f"Cycle link failed ({cycle_id}): {err}")

def create_plane_pages(plane_project_id, taiga_project, user_map):
    """Create Plane pages from Taiga wiki pages."""
    wiki_pages = taiga_project.get("wiki_pages", [])
    if wiki_pages:
        # Pages are not exposed via `/api/v1/...` (API key auth) on this Plane stack.
        # We prepare them in mapping for Layer 2 DB insertion.
        print(f"  📄 Preparing {len(wiki_pages)} wiki pages for Layer 2 DB migration...")
        prepared = 0
        for idx, page in enumerate(wiki_pages, start=1):
            slug = str(page.get("slug") or f"page-{idx}")
            key = f"{plane_project_id}:{slug}"
            owner_email = extract_author_email(page.get("owner"))
            last_modifier_email = extract_author_email(page.get("last_modifier")) or owner_email
            migration_mapping["pages"][key] = {
                "plane_id": None,
                "plane_project_id": plane_project_id,
                "slug": slug,
                "name": page.get("title") or slug_to_title(slug),
                "content": page.get("content") or "",
                "created_at": page.get("created_date"),
                "modified_at": page.get("modified_date"),
                "author_email": owner_email,
                "last_modifier_email": last_modifier_email,
            }
            stats["pages_prepared"] += 1
            prepared += 1
        print(f"    Prepared {prepared} wiki pages")
    return


# ==========================================
# MAIN MIGRATION ORCHESTRATOR
# ==========================================


def migrate_project(taiga_dump):
    """Orchestrate the full migration of one Taiga project dump."""
    project_name = taiga_dump.get("name", "Unknown")
    global CURRENT_TAIGA_SLUG
    CURRENT_TAIGA_SLUG = taiga_dump.get("slug")
    collect_taiga_emails(taiga_dump)
    print("=" * 60)
    print(f"🚀 MIGRATING PROJECT: {project_name}")
    print("=" * 60)

    detected_relations = relation_entries_count(taiga_dump)
    if detected_relations:
        stats["relations_detected"] += detected_relations
        msg = f"Detected {detected_relations} relation records in dump (not migrated yet)"
        print(f"  ⚠️  {msg}")
        stats["errors"].append(msg)

    # 1. Fetch Plane workspace users
    user_map = get_plane_users()

    # 2. Create Plane project
    plane_project_id = create_plane_project(taiga_dump)
    if not plane_project_id:
        print("❌ Cannot continue without a project. Aborting.")
        return
    collect_taiga_memberships(taiga_dump, plane_project_id)

    # 3. Create states from Taiga statuses
    state_map = create_plane_states(plane_project_id, taiga_dump)

    # 4. Create labels from Taiga tags
    label_map = create_plane_labels(plane_project_id, taiga_dump)

    # 5. Create modules from Taiga epics
    create_plane_modules(plane_project_id, taiga_dump, user_map)

    # 5b. Create cycles from Taiga milestones
    cycle_map = create_plane_cycles(plane_project_id, taiga_dump)

    # 5c. Ensure project members exist before creating issues so assignees can be applied.
    ensure_project_members(plane_project_id, taiga_dump, user_map)

    # 6. Create pages from Taiga wiki
    create_plane_pages(plane_project_id, taiga_dump, user_map)

    # 7. Migrate User Stories
    user_stories = taiga_dump.get("user_stories", [])
    print(f"\n  📝 Migrating {len(user_stories)} user stories...")
    for us in user_stories:
        plane_issue_id = create_plane_issue(
            plane_project_id, us, "Story", state_map, label_map, user_map
        )
        if plane_issue_id:
            create_plane_comments(plane_project_id, plane_issue_id, us, user_map, "Story")
            upload_attachments(plane_project_id, plane_issue_id, us, "Story")

    # 8. Migrate Tasks
    tasks = taiga_dump.get("tasks", [])
    print(f"\n  📋 Migrating {len(tasks)} tasks...")
    for task in tasks:
        parent_issue_id = None
        parent_story_ref = task.get("user_story")
        if MIGRATE_TASKS_AS_SUBISSUES and parent_story_ref is not None:
            story_key = f"Story_{parent_story_ref}"
            story_info = migration_mapping.get("issues", {}).get(story_key)
            if story_info and story_info.get("plane_id"):
                parent_issue_id = story_info["plane_id"]
            else:
                stats["errors"].append(
                    f"Task #{task.get('ref', '?')}: parent Story_{parent_story_ref} not found, created top-level"
                )
        plane_issue_id = create_plane_issue(
            plane_project_id,
            task,
            "Task",
            state_map,
            label_map,
            user_map,
            parent_issue_id=parent_issue_id,
            parent_story_ref=parent_story_ref if MIGRATE_TASKS_AS_SUBISSUES else None,
        )
        if plane_issue_id:
            create_plane_comments(plane_project_id, plane_issue_id, task, user_map, "Task")
            upload_attachments(plane_project_id, plane_issue_id, task, "Task")

    # 9. Migrate Issues
    issues = taiga_dump.get("issues", [])
    print(f"\n  🐛 Migrating {len(issues)} issues...")
    for issue in issues:
        plane_issue_id = create_plane_issue(
            plane_project_id, issue, "Issue", state_map, label_map, user_map
        )
        if plane_issue_id:
            create_plane_comments(plane_project_id, plane_issue_id, issue, user_map, "Issue")
            upload_attachments(plane_project_id, plane_issue_id, issue, "Issue")

    # 10. Link modules to issues using epic -> related stories
    link_modules_to_issues(plane_project_id)

    # 11. Link cycles to issues using milestone name on items
    link_cycles_to_issues(plane_project_id, taiga_dump, cycle_map)

    print("\n" + "=" * 60)
    print(f"✅ PROJECT MIGRATION COMPLETE: {project_name}")
    print("=" * 60)


# ==========================================
# ENTRY POINT
# ==========================================


def main():
    if len(sys.argv) < 2:
        print("Usage: python taiga_to_plane.py <taiga_dump.json> [<dump2.json> ...]")
        print("\nYou can pass multiple dump files to migrate several projects.")
        sys.exit(1)

    dump_files = sys.argv[1:]

    print("=" * 60)
    print("🔄 TAIGA → PLANE.SO MIGRATION (Layer 1: API)")
    print(f"   Plane URL: {PLANE_URL}")
    print(f"   Workspace: {PLANE_WORKSPACE_SLUG}")
    print(f"   Tasks as sub-issues: {'ON' if MIGRATE_TASKS_AS_SUBISSUES else 'OFF'}")
    print(f"   Dump files: {len(dump_files)}")
    print("=" * 60)

    for dump_file in dump_files:
        if not os.path.exists(dump_file):
            print(f"\n❌ File not found: {dump_file}")
            continue

        print(f"\n📂 Loading dump: {dump_file}")
        try:
            taiga_dump = load_taiga_dump(dump_file)
        except Exception as e:
            print(f"❌ Failed to load {dump_file}: {e}")
            continue

        migrate_project(taiga_dump)

    # Save mapping file
    mapping_output = "migration_mapping.json"
    with open(mapping_output, "w") as f:
        json.dump(migration_mapping, f, indent=2, default=str)
    print(f"\n📄 Mapping saved to: {mapping_output}")

    # Print summary
    print("\n" + "=" * 60)
    print("📊 MIGRATION SUMMARY")
    print("=" * 60)
    print(f"  Projects:    {stats['projects']}")
    print(f"  States:      {stats['states']}")
    print(f"  Labels:      {stats['labels']}")
    print(f"  Issues:      {stats['issues']}")
    print(f"  Comments:    {stats['comments']}")
    print(f"  Attachments: {stats['attachments']}")
    print(f"  Attach Dedup:{stats['attachments_deduped']}")
    print(f"  Attach Fail: {stats['attachments_failed']}")
    print(f"  Modules:     {stats['modules']}")
    print(f"  Module Links:{stats['module_links']}")
    print(f"  Cycles:      {stats['cycles']}")
    print(f"  Cycle Links: {stats['cycle_links']}")
    print(f"  Pages(API):  {stats['pages']}")
    print(f"  Pages Prep:  {stats['pages_prepared']}")
    print(f"  Relations:   {stats['relations_detected']}")
    print(f"  Errors:      {len(stats['errors'])}")

    if stats["errors"]:
        print("\n⚠️  ERRORS:")
        for err in stats["errors"]:
            print(f"    • {err}")
        if len(stats["errors"]) > 20:
            print(f"    ... and {len(stats['errors']) - 20} more")


if __name__ == "__main__":
    main()
