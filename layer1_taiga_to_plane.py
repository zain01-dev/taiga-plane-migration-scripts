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
import threading
import builtins
try:
    import markdown  # type: ignore
except Exception:  # pragma: no cover
    markdown = None
import base64
import gzip
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
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


def build_dump_output_paths(dump_file):
    """
    Build per-dump output paths so single-dump and multi-dump execution both
    isolate their logs, mapping file, and checkpoint file in the same folder.
    """
    dump_name = os.path.basename(dump_file)
    dump_stem, _ = os.path.splitext(dump_name)
    safe_stem = re.sub(r"[^A-Za-z0-9._-]+", "-", dump_stem).strip("-")
    safe_stem = safe_stem or "single_dump"
    output_dir = os.path.join(os.getcwd(), safe_stem)
    os.makedirs(output_dir, exist_ok=True)
    mapping_output = os.path.join(output_dir, f"migration_mapping_{safe_stem}.json")
    checkpoint_output = build_checkpoint_output_path(mapping_output)
    log_output = os.path.join(output_dir, "layer1.log")
    return {
        "dump_stem": safe_stem,
        "output_dir": output_dir,
        "mapping_output": mapping_output,
        "checkpoint_output": checkpoint_output,
        "log_output": log_output,
    }


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
LAYER1_WORKERS = max(1, int(os.environ.get("LAYER1_WORKERS", "4")))
ATTACHMENT_WORKERS = max(1, int(os.environ.get("LAYER1_ATTACHMENT_WORKERS", "2")))
CHECKPOINT_EVERY = max(1, int(os.environ.get("LAYER1_CHECKPOINT_EVERY", "10")))
LAYER1_RESUME = env_bool("LAYER1_RESUME", False)

# ==========================================
# GLOBALS
# ==========================================

PLANE_HEADERS = {"X-Api-Key": PLANE_API_KEY, "Content-Type": "application/json"}

if not PLANE_URL or not PLANE_API_KEY or not PLANE_WORKSPACE_SLUG:
    print("❌ Missing configuration.")
    print("Set env vars: PLANE_URL, PLANE_API_KEY, PLANE_WORKSPACE_SLUG")
    sys.exit(1)

DEFAULT_MIGRATION_MAPPING = {
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

DEFAULT_STATS = {
    "projects": 0,
    "states": 0,
    "labels": 0,
    "issues": 0,
    "comments": 0,
    "attachments": 0,
    "attachments_deduped": 0,
    "attachments_checkpoint_skipped": 0,
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


def fresh_mapping():
    """Create a brand-new mapping container for one dump run."""
    return {
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


def fresh_stats():
    """Create fresh per-run stats so dumps never contaminate each other."""
    return {
        "projects": 0,
        "states": 0,
        "labels": 0,
        "issues": 0,
        "comments": 0,
        "attachments": 0,
        "attachments_deduped": 0,
        "attachments_checkpoint_skipped": 0,
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


migration_mapping = fresh_mapping()
stats = fresh_stats()

CURRENT_TAIGA_SLUG = None
RUN_CONTEXT = {
    "mapping_output": None,
    "checkpoint_output": None,
    "log_output": None,
    "log_handle": None,
    "dump_file": None,
    "completed_stages": [],
    "plane_project_id": None,
    "project_slug": None,
    "project_name": None,
}
STATS_LOCK = threading.Lock()
MAPPING_LOCK = threading.Lock()
CHECKPOINT_LOCK = threading.Lock()
SESSION_LOCAL = threading.local()
PRINT_LOCK = threading.Lock()

# ==========================================
# UTILITY FUNCTIONS
# ==========================================


def print(*args, **kwargs):
    """
    Thread-aware console output so parallel workers remain readable in one shell.
    Every line is prefixed with local time and the worker thread name.
    """
    sep = kwargs.pop("sep", " ")
    end = kwargs.pop("end", "\n")
    file = kwargs.pop("file", sys.stdout)
    flush = kwargs.pop("flush", True)
    timestamp = datetime.now().strftime("%H:%M:%S")
    raw_thread_name = threading.current_thread().name or "main"

    if raw_thread_name == "MainThread":
        display_thread_name = "Main Thread"
    else:
        match = re.match(r"^(.*)_(\d+)$", raw_thread_name)
        if match:
            stage_name = match.group(1).replace("_", " ").strip()
            worker_number = int(match.group(2)) + 1
            display_thread_name = f"Worker Thread {worker_number} ({stage_name})"
        else:
            display_thread_name = raw_thread_name.replace("_", " ").strip()

    prefix = f"[{timestamp}][{display_thread_name}] "
    message = sep.join(str(arg) for arg in args)
    with PRINT_LOCK:
        rendered = prefix + message
        builtins.print(rendered, sep="", end=end, file=file, flush=flush, **kwargs)
        log_handle = RUN_CONTEXT.get("log_handle")
        if log_handle is not None and file in (sys.stdout, sys.stderr):
            builtins.print(rendered, sep="", end=end, file=log_handle, flush=True)


def get_session():
    """Keep one requests.Session per worker thread for connection reuse."""
    session = getattr(SESSION_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        SESSION_LOCAL.session = session
    return session


def update_stat(key, delta=1):
    """Thread-safe stats increment helper."""
    with STATS_LOCK:
        stats[key] += delta


def record_error(message):
    """Thread-safe central error collection."""
    with STATS_LOCK:
        stats["errors"].append(message)


def atomic_write_json(path, data):
    """Write JSON atomically so checkpoints are never left half-written."""
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    os.replace(tmp_path, path)


def close_run_log():
    """Close the active per-dump log file, if one is open."""
    handle = RUN_CONTEXT.get("log_handle")
    if handle is None:
        return
    try:
        handle.flush()
        handle.close()
    finally:
        RUN_CONTEXT["log_handle"] = None


def reset_run_state():
    """Reset all mutable globals so each dump starts with isolated state."""
    global migration_mapping, stats, CURRENT_TAIGA_SLUG
    close_run_log()
    migration_mapping = fresh_mapping()
    stats = fresh_stats()
    CURRENT_TAIGA_SLUG = None
    RUN_CONTEXT.update(
        {
            "mapping_output": None,
            "checkpoint_output": None,
            "log_output": None,
            "log_handle": None,
            "dump_file": None,
            "completed_stages": [],
            "plane_project_id": None,
            "project_slug": None,
            "project_name": None,
        }
    )
    session = getattr(SESSION_LOCAL, "session", None)
    if session is not None:
        try:
            session.close()
        except Exception:
            pass
        SESSION_LOCAL.session = None


def build_checkpoint_output_path(mapping_output):
    stem, ext = os.path.splitext(mapping_output)
    return f"{stem}.checkpoint{ext or '.json'}"


def save_run_checkpoint(stage_name=None):
    """
    Persist the current in-memory mapping plus run progress.
    This is our restart point for long single-dump migrations.
    """
    mapping_output = RUN_CONTEXT.get("mapping_output")
    checkpoint_output = RUN_CONTEXT.get("checkpoint_output")
    if not mapping_output or not checkpoint_output:
        return

    with CHECKPOINT_LOCK:
        if stage_name and stage_name not in RUN_CONTEXT["completed_stages"]:
            RUN_CONTEXT["completed_stages"].append(stage_name)
        with MAPPING_LOCK:
            atomic_write_json(mapping_output, migration_mapping)
        atomic_write_json(
            checkpoint_output,
            {
                "dump_file": RUN_CONTEXT.get("dump_file"),
                "project_slug": RUN_CONTEXT.get("project_slug"),
                "project_name": RUN_CONTEXT.get("project_name"),
                "plane_project_id": RUN_CONTEXT.get("plane_project_id"),
                "completed_stages": RUN_CONTEXT.get("completed_stages", []),
                "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            },
        )


def hydrate_stats_from_mapping():
    """Best-effort stats rebuild when resuming from an existing checkpoint."""
    with STATS_LOCK:
        stats["projects"] = len(migration_mapping.get("projects", {}))
        stats["issues"] = len(migration_mapping.get("issues", {}))
        stats["comments"] = len(migration_mapping.get("comments", {}))
        stats["attachments"] = len(migration_mapping.get("attachments", {}))
        stats["attachments_checkpoint_skipped"] = 0
        stats["modules"] = len(migration_mapping.get("modules", {}))
        stats["cycles"] = len(migration_mapping.get("cycles", {}))
        stats["pages_prepared"] = len(migration_mapping.get("pages", {}))
        stats["states"] = sum(len(v) for v in migration_mapping.get("states", {}).values())
        stats["labels"] = sum(len(v) for v in migration_mapping.get("labels", {}).values())


def stage_completed(stage_name):
    """Return True when a stage has already been checkpointed as complete."""
    return stage_name in RUN_CONTEXT.get("completed_stages", [])


def format_elapsed(seconds):
    """Format elapsed seconds into a readable h/m/s string."""
    seconds = int(max(0, round(seconds)))
    hours, rem = divmod(seconds, 3600)
    minutes, secs = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def snapshot_stats():
    """Take an immutable copy of the current per-dump stats."""
    with STATS_LOCK:
        copied = {}
        for key, value in stats.items():
            copied[key] = list(value) if isinstance(value, list) else value
    return copied


def accumulate_stats(total, current):
    """Fold one dump's summary into the multi-dump aggregate summary."""
    for key, value in current.items():
        if key == "errors":
            total["errors"].extend(value)
        else:
            total[key] += value


def print_summary(summary, title="MIGRATION SUMMARY", total_time=None):
    """Render a human-readable summary for one dump or an aggregate run."""
    print("\n" + "=" * 60)
    print(f"📊 {title}")
    print("=" * 60)
    print(f"  Projects:    {summary['projects']}")
    print(f"  States:      {summary['states']}")
    print(f"  Labels:      {summary['labels']}")
    print(f"  Issues:      {summary['issues']}")
    print(f"  Comments:    {summary['comments']}")
    print(f"  Attachments: {summary['attachments']}")
    print(f"  Attach Dedup:{summary['attachments_deduped']}")
    print(f"  Attach Skip: {summary['attachments_checkpoint_skipped']}")
    print(f"  Attach Fail: {summary['attachments_failed']}")
    print(f"  Modules:     {summary['modules']}")
    print(f"  Module Links:{summary['module_links']}")
    print(f"  Cycles:      {summary['cycles']}")
    print(f"  Cycle Links: {summary['cycle_links']}")
    print(f"  Pages(API):  {summary['pages']}")
    print(f"  Pages Prep:  {summary['pages_prepared']}")
    print(f"  Relations:   {summary['relations_detected']}")
    print(f"  Errors:      {len(summary['errors'])}")
    if total_time is not None:
        print(f"  Total Time:  {format_elapsed(total_time)}")


def api_call(method, url, json_data=None, files=None, retries=3):
    """Make an API call to Plane.so with retry logic."""
    time.sleep(RATE_LIMIT_DELAY)
    headers = dict(PLANE_HEADERS)
    if files:
        # Don't send Content-Type header when uploading files
        headers.pop("Content-Type", None)

    for attempt in range(retries):
        try:
            resp = get_session().request(
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


def build_attachment_source_key(item_type, taiga_ref, attachment, attachment_index):
    """
    Build a stable identity for one source attachment occurrence.
    We intentionally include item context plus attachment position/order so that
    different attachments with the same filename or sha1 are never collapsed.
    """
    filename = attachment.get("name") or attachment.get("filename") or "attachment"
    attachment_id = attachment.get("id")
    order = attachment.get("order")
    sha1 = attachment.get("sha1")
    size = attachment.get("size")
    created_at = attachment.get("created_date")
    modified_at = attachment.get("modified_date")
    return "|".join(
        [
            str(item_type or "Item"),
            str(taiga_ref),
            str(attachment_index),
            str(order if order is not None else ""),
            str(attachment_id if attachment_id is not None else ""),
            str(filename),
            str(sha1 if sha1 is not None else ""),
            str(size if size is not None else ""),
            str(created_at if created_at is not None else ""),
            str(modified_at if modified_at is not None else ""),
        ]
    )


def attachment_mapping_exists(plane_project_id, plane_issue_id, source_key, attachment=None, filename=None):
    """Resume-safe attachment lookup using the exact source attachment identity."""
    expected_sha1 = str(attachment.get("sha1")) if attachment and attachment.get("sha1") else None
    expected_size = attachment.get("size") if attachment else None
    expected_created_at = attachment.get("created_date") if attachment else None
    expected_order = attachment.get("order") if attachment else None
    expected_attachment_id = attachment.get("id") if attachment else None
    with MAPPING_LOCK:
        for info in migration_mapping.get("attachments", {}).values():
            if not isinstance(info, dict):
                continue
            if info.get("plane_project_id") != plane_project_id:
                continue
            if info.get("plane_issue_id") != plane_issue_id:
                continue
            if info.get("attachment_source_key") == source_key:
                return True
            # Backward-compatible fallback for checkpoints created before source-key support.
            # We require multiple fields to line up together to avoid false skips on generic
            # names like "attached_file" or "image.png".
            if not attachment:
                continue
            if filename and info.get("filename") != filename:
                continue
            if expected_sha1 and info.get("attachment_sha1") != expected_sha1:
                continue
            if expected_size is not None and info.get("size") != expected_size:
                continue
            if expected_created_at and info.get("created_at") != expected_created_at:
                continue
            legacy_order = info.get("attachment_order")
            legacy_attachment_id = info.get("attachment_id")
            if expected_order is not None and legacy_order not in (None, expected_order):
                continue
            if expected_attachment_id is not None and legacy_attachment_id not in (None, expected_attachment_id):
                continue
            if filename:
                return True
    return False


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
    project_slug = taiga_project.get("slug") or project_name

    # Resume safety: if this project was already checkpointed, reuse it.
    with MAPPING_LOCK:
        existing = migration_mapping["projects"].get(project_slug)
    if isinstance(existing, dict) and existing.get("plane_id"):
        plane_project_id = existing["plane_id"]
        RUN_CONTEXT["plane_project_id"] = plane_project_id
        RUN_CONTEXT["project_slug"] = project_slug
        RUN_CONTEXT["project_name"] = existing.get("name", project_name)
        print(f"\n📁 Reusing project: {RUN_CONTEXT['project_name']} (ID: {plane_project_id})")
        return plane_project_id

    print(f"\n📁 Creating project: {project_name}")

    # Plane requires a unique project identifier (<= 12 chars) per workspace.
    slug = project_slug
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
        with MAPPING_LOCK:
            migration_mapping["projects"][project_slug] = {
                "plane_id": plane_project_id,
                "name": created_name,
            }
        RUN_CONTEXT["plane_project_id"] = plane_project_id
        RUN_CONTEXT["project_slug"] = project_slug
        RUN_CONTEXT["project_name"] = created_name
        update_stat("projects")
        return plane_project_id
    else:
        err = resp.text if resp is not None else "No response"
        print(f"  ❌ Failed to create project: {err}")
        record_error(f"Project creation failed: {project_name} — {err}")
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
            update_stat("project_members")
        else:
            err = resp.text if resp is not None else "No response"
            record_error(f"Project member create failed ({email}): {err}")

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
    mapping_key = f"{item_type}_{taiga_ref}"

    # Resume safety: if this issue was already checkpointed, reuse it.
    with MAPPING_LOCK:
        existing = migration_mapping["issues"].get(mapping_key)
    if isinstance(existing, dict) and existing.get("plane_project_id") == plane_project_id and existing.get("plane_id"):
        print(f"    ↩️  Reusing {item_type} #{taiga_ref}: {subject[:50]}")
        return existing["plane_id"]

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
            record_error(
                f"Task #{taiga_ref}: parent link rejected by API, created as top-level issue"
            )
            parent_issue_id = None

    if resp is not None and resp.status_code in (200, 201):
        plane_issue = resp.json()
        plane_issue_id = plane_issue["id"]
        print(f"    ✅ Issue #{taiga_ref}: {subject[:50]}")

        # Store mapping with metadata for Layer 2
        with MAPPING_LOCK:
            migration_mapping["issues"][mapping_key] = {
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
        update_stat("issues")
        return plane_issue_id
    else:
        err = resp.text if resp is not None else "No response"
        print(f"    ❌ Issue #{taiga_ref} failed: {err}")
        record_error(f"Issue #{taiga_ref} ({item_type}): {err}")
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

        with MAPPING_LOCK:
            if synthetic_id in migration_mapping["comments"]:
                continue

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
            with MAPPING_LOCK:
                migration_mapping["comments"][synthetic_id] = {
                    "plane_id": plane_comment_id,
                    "plane_issue_id": plane_issue_id,
                    "plane_project_id": plane_project_id,
                    "created_at": created_at,
                    "author_email": author_email if author_email != "Unknown" else "",
                }
            update_stat("comments")
        else:
            err = resp.text if resp is not None else "No response"
            print(f"      ⚠️  Comment on #{taiga_ref} failed: {err}")
            record_error(f"Comment on #{taiga_ref}: {err}")


def upload_attachments(plane_project_id, plane_issue_id, item, item_type=None):
    """Upload attachments from Taiga to Plane."""
    attachments = item.get("attachments", [])
    taiga_ref = item.get("ref", "?")
    seen_attachment_keys = set()

    for attachment_index, attachment in enumerate(attachments):
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
                update_stat("attachments_failed")
                continue
            try:
                file_content = base64.b64decode(data)
            except Exception as e:
                print(f"      ⚠️  Attachment base64 decode failed ({filename}): {e}")
                update_stat("attachments_failed")
                continue
        elif isinstance(attached_file, str):
            if not filename:
                filename = os.path.basename(attached_file) or "attachment"
            if attached_file.startswith("http"):
                try:
                    file_resp = get_session().get(attached_file, timeout=30)
                    if file_resp.status_code != 200:
                        print(f"      ⚠️  Could not download attachment: {filename}")
                        update_stat("attachments_failed")
                        continue
                    file_content = file_resp.content
                except Exception as e:
                    print(f"      ⚠️  Attachment download error ({filename}): {e}")
                    update_stat("attachments_failed")
                    continue
            else:
                print(f"      ⚠️  Skipping unsupported attachment reference: {filename}")
                update_stat("attachments_failed")
                continue
        else:
            print(f"      ⚠️  Skipping unsupported attachment type: {type(attached_file).__name__}")
            update_stat("attachments_failed")
            continue

        content_type = attachment.get("content_type") or mimetypes.guess_type(filename)[0]
        if not content_type:
            try:
                file_content.decode("utf-8")
                content_type = "text/plain"
            except Exception:
                content_type = "application/octet-stream"

        dedupe_key = (
            build_attachment_source_key(item_type or "Item", taiga_ref, attachment, attachment_index)
        )
        if attachment_mapping_exists(
            plane_project_id,
            plane_issue_id,
            dedupe_key,
            attachment=attachment,
            filename=filename,
        ):
            print(f"      ↩️  Attachment already checkpointed: {filename}")
            update_stat("attachments_checkpoint_skipped")
            continue
        if dedupe_key in seen_attachment_keys:
            print(f"      📎 Duplicate attachment skipped: {filename}")
            update_stat("attachments_deduped")
            continue
        seen_attachment_keys.add(dedupe_key)

        # Plane uses a presigned upload flow for issue attachments:
        # 1) create attachment record (returns presigned POST fields)
        # 2) upload file to storage using the presigned POST
        # external_id must be unique per source attachment occurrence, not just per sha1,
        # otherwise the same file attached to different issues can be collapsed incorrectly.
        meta_payload = {
            "name": filename,
            "type": content_type,
            "size": len(file_content),
            "external_source": "taiga",
            "external_id": dedupe_key,
        }

        meta_resp = api_call(
            "POST",
            f"workspaces/{PLANE_WORKSPACE_SLUG}/projects/{plane_project_id}/issues/{plane_issue_id}/issue-attachments/",
            json_data=meta_payload,
        )

        if meta_resp is not None and meta_resp.status_code == 409:
            print(f"      📎 Attachment already exists: {filename}")
            update_stat("attachments_deduped")
            continue

        if meta_resp is None or meta_resp.status_code not in (200, 201):
            err = meta_resp.text if meta_resp is not None else "No response"
            print(f"      ⚠️  Attachment metadata failed ({filename}): {err}")
            record_error(f"Attachment metadata failed: #{taiga_ref} {filename} — {err}")
            update_stat("attachments_failed")
            continue

        try:
            meta_json = meta_resp.json()
            upload_data = meta_json.get("upload_data") or {}
            upload_url = upload_data.get("url")
            upload_fields = upload_data.get("fields") or {}
            asset_id = meta_json.get("asset_id") or (meta_json.get("attachment") or {}).get("id")
        except Exception as e:
            print(f"      ⚠️  Attachment metadata parse failed ({filename}): {e}")
            record_error(f"Attachment metadata parse failed: #{taiga_ref} {filename} — {e}")
            update_stat("attachments_failed")
            continue

        if not upload_url or not upload_fields:
            print(f"      ⚠️  Missing presigned upload data ({filename})")
            record_error(f"Attachment presign missing: #{taiga_ref} {filename}")
            update_stat("attachments_failed")
            continue

        upload_url = normalize_upload_url(upload_url)

        try:
            upload_resp = get_session().post(
                upload_url,
                data=upload_fields,
                files={"file": (filename, file_content, content_type)},
                timeout=60,
            )
        except Exception as e:
            print(f"      ⚠️  Attachment upload request failed ({filename}): {e}")
            record_error(f"Attachment upload request failed: #{taiga_ref} {filename} — {e}")
            update_stat("attachments_failed")
            continue

        if upload_resp.status_code in (200, 201, 204):
            print(f"      📎 Uploaded: {filename}")
            update_stat("attachments")
            if asset_id:
                with MAPPING_LOCK:
                    map_key = f"{item_type or 'Item'}_{taiga_ref}_{attachment.get('sha1') or filename}_{len(migration_mapping['attachments'])}"
                    migration_mapping["attachments"][map_key] = {
                        "asset_id": asset_id,
                        "plane_issue_id": plane_issue_id,
                        "plane_project_id": plane_project_id,
                        "taiga_ref": taiga_ref,
                        "taiga_type": item_type or "Item",
                        "filename": filename,
                        "attachment_source_key": dedupe_key,
                        "attachment_index": attachment_index,
                        "attachment_order": attachment.get("order"),
                        "attachment_id": attachment.get("id"),
                        "attachment_sha1": str(attachment.get("sha1")) if attachment.get("sha1") else None,
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
                    record_error(
                        f"Attachment confirm failed: #{taiga_ref} {filename} — {err}"
                    )
        else:
            print(f"      ⚠️  Attachment upload failed ({filename}): HTTP {upload_resp.status_code}")
            record_error(
                f"Attachment upload failed: #{taiga_ref} {filename} — HTTP {upload_resp.status_code}"
            )
            update_stat("attachments_failed")


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
            record_error(f"Module (Epic #{ref}) failed: {err}")

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
            record_error(f"Module link failed ({module_key}): {err}")

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
        record_error(f"Cycle creation failed ({name}): {err}")

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
            record_error(f"Cycle link failed ({cycle_id}): {err}")

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


def build_cycle_map_from_mapping(plane_project_id):
    """Reconstruct cycle name -> id mapping from checkpointed Layer 1 state."""
    cycle_map = {}
    with MAPPING_LOCK:
        for key, info in migration_mapping.get("cycles", {}).items():
            if not isinstance(info, dict):
                continue
            if info.get("plane_project_id") != plane_project_id:
                continue
            if ":" not in key:
                continue
            cycle_name = key.split(":", 1)[1]
            if info.get("plane_id"):
                cycle_map[cycle_name] = info["plane_id"]
    return cycle_map


def process_issue_tree_item(worker_label, plane_project_id, item, state_map, label_map, user_map, parent_issue_id=None, parent_story_ref=None):
    """
    Worker-safe item migration unit:
    create the issue, then its comments.
    Attachments are deferred to a lower-concurrency stage so large uploads don't
    overwhelm the server while item creation is running at four workers.
    """
    plane_issue_id = create_plane_issue(
        plane_project_id,
        item,
        worker_label,
        state_map,
        label_map,
        user_map,
        parent_issue_id=parent_issue_id,
        parent_story_ref=parent_story_ref,
    )
    if plane_issue_id:
        create_plane_comments(plane_project_id, plane_issue_id, item, user_map, worker_label)
        return {
            "plane_project_id": plane_project_id,
            "plane_issue_id": plane_issue_id,
            "item": item,
            "item_type": worker_label,
        }
    return None


def process_attachment_job(job):
    """Upload attachments for one already-created issue."""
    if not job:
        return None
    upload_attachments(
        job["plane_project_id"],
        job["plane_issue_id"],
        job["item"],
        job["item_type"],
    )
    return True


def build_attachment_jobs_from_mapping(plane_project_id, items, item_type):
    """
    Rebuild attachment jobs from the dump plus issue mappings.

    This is the key resume-safety path for attachment stages: even if an older
    checkpoint marked an attachment stage complete too early, we can still walk
    the source dump again and upload any attachment that is not yet present in
    migration_mapping.
    """
    jobs = []
    unmapped_items = 0

    for item in items:
        attachments = item.get("attachments") or []
        if not attachments:
            continue

        issue_info = find_issue_mapping(plane_project_id, item_type, item.get("ref"))
        plane_issue_id = issue_info.get("plane_id") if issue_info else None
        if not plane_issue_id:
            unmapped_items += 1
            continue

        jobs.append(
            {
                "plane_project_id": plane_project_id,
                "plane_issue_id": plane_issue_id,
                "item": item,
                "item_type": item_type,
            }
        )

    if unmapped_items:
        print(
            f"    ⚠️  {unmapped_items} {item_type} items with attachments could not be rebuilt "
            f"from checkpoint because their Plane issue mapping is missing"
        )

    return jobs


def run_parallel_stage(stage_name, items, worker_fn, max_workers=None, allow_resume_skip=True):
    """
    Run one independent stage with bounded thread concurrency.
    We checkpoint after every N completed items so long migrations can resume.
    """
    if allow_resume_skip and stage_completed(stage_name):
        print(f"\n  ↩️  Skipping {stage_name}: already checkpointed")
        return []

    if not items:
        save_run_checkpoint(stage_name)
        return []

    worker_count = max_workers or LAYER1_WORKERS
    stage_start = time.monotonic()
    print(f"\n  ⚙️  {stage_name}: {len(items)} items with {worker_count} workers")
    results = []
    completed = 0
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix=stage_name[:12]) as executor:
        futures = [executor.submit(worker_fn, item) for item in items]
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:  # pragma: no cover - defensive checkpoint path
                record_error(f"{stage_name} worker crashed: {exc}")
            completed += 1
            if completed % CHECKPOINT_EVERY == 0:
                print(f"    💾 Checkpoint after {completed}/{len(items)} {stage_name}")
                save_run_checkpoint()

    save_run_checkpoint(stage_name)
    print(f"    ✅ Completed {stage_name} in {format_elapsed(time.monotonic() - stage_start)}")
    return results


# ==========================================
# MAIN MIGRATION ORCHESTRATOR
# ==========================================


def migrate_project(taiga_dump):
    """
    Orchestrate the full migration of one Taiga project dump.
    For production-scale runs we keep strict stage barriers:
    project setup -> stories -> tasks -> issues -> links.
    """
    project_start = time.monotonic()
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
        record_error(msg)

    # Always refresh workspace users because Layer 0 / manual user fixes may have changed them.
    user_map = get_plane_users()

    # Stage 1: project scaffolding. We keep this serial because later stages depend on it.
    if stage_completed("project_setup") and RUN_CONTEXT.get("plane_project_id"):
        plane_project_id = RUN_CONTEXT["plane_project_id"]
        state_map = migration_mapping.get("states", {}).get(plane_project_id, {})
        label_map = migration_mapping.get("labels", {}).get(plane_project_id, {})
        cycle_map = build_cycle_map_from_mapping(plane_project_id)
        print(f"  ↩️  Reusing checkpointed project setup for: {project_name}")
    else:
        plane_project_id = create_plane_project(taiga_dump)
        if not plane_project_id:
            print("❌ Cannot continue without a project. Aborting.")
            return
        collect_taiga_memberships(taiga_dump, plane_project_id)
        state_map = create_plane_states(plane_project_id, taiga_dump)
        label_map = create_plane_labels(plane_project_id, taiga_dump)
        create_plane_modules(plane_project_id, taiga_dump, user_map)
        cycle_map = create_plane_cycles(plane_project_id, taiga_dump)
        # Assignees only stick when project members already exist.
        ensure_project_members(plane_project_id, taiga_dump, user_map)
        create_plane_pages(plane_project_id, taiga_dump, user_map)
        save_run_checkpoint("project_setup")

    # Stage 2: stories first so tasks can safely attach to known parent story ids.
    user_stories = taiga_dump.get("user_stories", [])
    print(f"\n  📝 Migrating {len(user_stories)} user stories...")
    story_jobs_result = run_parallel_stage(
        "user_stories",
        user_stories,
        lambda us: process_issue_tree_item("Story", plane_project_id, us, state_map, label_map, user_map),
    )
    if not story_jobs_result and user_stories:
        # Attachment stages are intentionally re-runnable on resume. We rebuild
        # jobs from the dump + issue mappings so a stale checkpoint cannot hide
        # attachments that never actually uploaded.
        story_attachment_jobs = build_attachment_jobs_from_mapping(plane_project_id, user_stories, "Story")
    else:
        story_attachment_jobs = [job for job in story_jobs_result if job]
    run_parallel_stage(
        "user_story_attachments",
        story_attachment_jobs,
        process_attachment_job,
        max_workers=ATTACHMENT_WORKERS,
        allow_resume_skip=False,
    )

    # Stage 3: tasks after stories. Parent lookup uses already-created story mappings.
    tasks = taiga_dump.get("tasks", [])
    print(f"\n  📋 Migrating {len(tasks)} tasks...")

    def task_worker(task):
        parent_issue_id = None
        parent_story_ref = task.get("user_story")
        if MIGRATE_TASKS_AS_SUBISSUES and parent_story_ref is not None:
            story_info = find_issue_mapping(plane_project_id, "Story", parent_story_ref)
            if story_info and story_info.get("plane_id"):
                parent_issue_id = story_info["plane_id"]
            else:
                record_error(
                    f"Task #{task.get('ref', '?')}: parent Story_{parent_story_ref} not found, created top-level"
                )
        return process_issue_tree_item(
            "Task",
            plane_project_id,
            task,
            state_map,
            label_map,
            user_map,
            parent_issue_id=parent_issue_id,
            parent_story_ref=parent_story_ref if MIGRATE_TASKS_AS_SUBISSUES else None,
        )
    task_jobs_result = run_parallel_stage("tasks", tasks, task_worker)
    if not task_jobs_result and tasks:
        task_attachment_jobs = build_attachment_jobs_from_mapping(plane_project_id, tasks, "Task")
    else:
        task_attachment_jobs = [job for job in task_jobs_result if job]
    run_parallel_stage(
        "task_attachments",
        task_attachment_jobs,
        process_attachment_job,
        max_workers=ATTACHMENT_WORKERS,
        allow_resume_skip=False,
    )

    # Stage 4: standalone issues can now run independently in parallel.
    issues = taiga_dump.get("issues", [])
    print(f"\n  🐛 Migrating {len(issues)} issues...")
    issue_jobs_result = run_parallel_stage(
        "issues",
        issues,
        lambda issue: process_issue_tree_item("Issue", plane_project_id, issue, state_map, label_map, user_map),
    )
    if not issue_jobs_result and issues:
        issue_attachment_jobs = build_attachment_jobs_from_mapping(plane_project_id, issues, "Issue")
    else:
        issue_attachment_jobs = [job for job in issue_jobs_result if job]
    run_parallel_stage(
        "issue_attachments",
        issue_attachment_jobs,
        process_attachment_job,
        max_workers=ATTACHMENT_WORKERS,
        allow_resume_skip=False,
    )

    # Final serial linking depends on all item ids already existing.
    if not stage_completed("module_links"):
        link_modules_to_issues(plane_project_id)
        save_run_checkpoint("module_links")

    if not stage_completed("cycle_links"):
        link_cycles_to_issues(plane_project_id, taiga_dump, cycle_map)
        save_run_checkpoint("cycle_links")

    print("\n" + "=" * 60)
    print(f"✅ PROJECT MIGRATION COMPLETE: {project_name}")
    print(f"⏱️  Project time: {format_elapsed(time.monotonic() - project_start)}")
    print("=" * 60)


# ==========================================
# ENTRY POINT
# ==========================================


def load_resume_state(mapping_output, checkpoint_output):
    """Hydrate the current dump run from its own mapping/checkpoint pair."""
    print(f"\n↩️  Resuming from checkpoint: {checkpoint_output}")
    with open(mapping_output, "r", encoding="utf-8") as f:
        loaded_mapping = json.load(f)
    migration_mapping.clear()
    migration_mapping.update(loaded_mapping)
    with open(checkpoint_output, "r", encoding="utf-8") as f:
        checkpoint_state = json.load(f)
    RUN_CONTEXT["completed_stages"] = list(checkpoint_state.get("completed_stages", []))
    RUN_CONTEXT["plane_project_id"] = checkpoint_state.get("plane_project_id")
    RUN_CONTEXT["project_slug"] = checkpoint_state.get("project_slug")
    RUN_CONTEXT["project_name"] = checkpoint_state.get("project_name")
    hydrate_stats_from_mapping()


def run_single_dump(dump_file):
    """
    Run one dump end-to-end with fully isolated state, outputs, and logging.
    Multi-dump mode simply loops over this function so each dump behaves like
    a normal single-dump migration.
    """
    run_start = time.monotonic()
    reset_run_state()

    output_paths = build_dump_output_paths(dump_file)
    RUN_CONTEXT["mapping_output"] = output_paths["mapping_output"]
    RUN_CONTEXT["checkpoint_output"] = output_paths["checkpoint_output"]
    RUN_CONTEXT["log_output"] = output_paths["log_output"]
    RUN_CONTEXT["dump_file"] = dump_file
    log_mode = "a" if LAYER1_RESUME and os.path.exists(output_paths["log_output"]) else "w"
    RUN_CONTEXT["log_handle"] = open(output_paths["log_output"], log_mode, encoding="utf-8")

    print("=" * 60)
    print("🔄 TAIGA → PLANE.SO MIGRATION (Layer 1: API)")
    print(f"   Plane URL: {PLANE_URL}")
    print(f"   Workspace: {PLANE_WORKSPACE_SLUG}")
    print(f"   Tasks as sub-issues: {'ON' if MIGRATE_TASKS_AS_SUBISSUES else 'OFF'}")
    print("   Dump files: 1")
    print(f"   Workers: {LAYER1_WORKERS}")
    print(f"   Attachment workers: {ATTACHMENT_WORKERS}")
    print(f"   Checkpoint every: {CHECKPOINT_EVERY} items")
    print(f"   Resume mode: {'ON' if LAYER1_RESUME else 'OFF'}")
    print(f"   Output dir: {output_paths['output_dir']}")
    print(f"   Log file: {output_paths['log_output']}")
    print("=" * 60)

    try:
        if LAYER1_RESUME and os.path.exists(output_paths["mapping_output"]) and os.path.exists(output_paths["checkpoint_output"]):
            load_resume_state(output_paths["mapping_output"], output_paths["checkpoint_output"])

        if not os.path.exists(dump_file):
            print(f"\n❌ File not found: {dump_file}")
            record_error(f"File not found: {dump_file}")
            return {"stats": snapshot_stats(), "output_paths": output_paths}

        print(f"\n📂 Loading dump: {dump_file}")
        try:
            taiga_dump = load_taiga_dump(dump_file)
        except Exception as e:
            print(f"❌ Failed to load {dump_file}: {e}")
            record_error(f"Failed to load dump {dump_file}: {e}")
            return {"stats": snapshot_stats(), "output_paths": output_paths}

        migrate_project(taiga_dump)

        save_run_checkpoint()
        print(f"\n📄 Mapping saved to: {output_paths['mapping_output']}")

        run_stats = snapshot_stats()
        print_summary(run_stats, total_time=time.monotonic() - run_start)

        if run_stats["errors"]:
            print("\n⚠️  ERRORS:")
            for err in run_stats["errors"]:
                print(f"    • {err}")

        return {"stats": run_stats, "output_paths": output_paths}
    finally:
        close_run_log()


def main():
    overall_start = time.monotonic()
    if len(sys.argv) < 2:
        print("Usage: python taiga_to_plane.py <taiga_dump.json> [<dump2.json> ...]")
        print("\nYou can pass multiple dump files to migrate several projects.")
        sys.exit(1)

    dump_files = sys.argv[1:]
    aggregate_stats = fresh_stats()

    print("=" * 60)
    print("🔄 TAIGA → PLANE.SO MIGRATION (Layer 1: API)")
    print(f"   Plane URL: {PLANE_URL}")
    print(f"   Workspace: {PLANE_WORKSPACE_SLUG}")
    print(f"   Tasks as sub-issues: {'ON' if MIGRATE_TASKS_AS_SUBISSUES else 'OFF'}")
    print(f"   Dump files: {len(dump_files)}")
    print(f"   Workers: {LAYER1_WORKERS}")
    print(f"   Attachment workers: {ATTACHMENT_WORKERS}")
    print(f"   Checkpoint every: {CHECKPOINT_EVERY} items")
    print(f"   Resume mode: {'ON' if LAYER1_RESUME else 'OFF'}")
    print("=" * 60)

    for index, dump_file in enumerate(dump_files, start=1):
        print(f"\n🚚 Starting dump {index}/{len(dump_files)}")
        result = run_single_dump(dump_file)
        accumulate_stats(aggregate_stats, result["stats"])

    if len(dump_files) > 1:
        print_summary(
            aggregate_stats,
            title="MULTI-DUMP SUMMARY",
            total_time=time.monotonic() - overall_start,
        )

        if aggregate_stats["errors"]:
            print("\n⚠️  ERRORS:")
            for err in aggregate_stats["errors"]:
                print(f"    • {err}")


if __name__ == "__main__":
    main()
