# Taiga -> Plane Migration Scripts

This repo is the clean shareable bundle for production migration.

## Included Files
- `layer0_user_presync.py`
- `taiga_user_sync.py`
- `layer1_taiga_to_plane.py`
- `layer2_update_plane_db.py`
- `README.md`

## What Each Layer Does
### Layer 0
Pre-sync users from Taiga into Plane:
- fetches Taiga users, memberships, roles
- creates missing Plane users
- creates/updates workspace memberships

### Layer 1
Migrates dump data through Plane API:
- projects
- states
- labels
- project members
- issues/stories/tasks
- assignees
- comments
- attachments
- module links
- watcher data into mapping
- generates a fresh per-dump mapping and checkpoint set

### Layer 2
Runs DB reconciliation using Layer 1 mapping:
- timestamps
- authorship
- project memberships and roles
- watcher -> subscriber sync
- pages/wiki DB restore
- other DB-side metadata

## Dependencies
Install:
```bash
python3 -m pip install requests "psycopg[binary]"
```

## Required Environment Variables
```bash
export TAIGA_API_URL="https://projects.arbisoft.com/api/v1"
export TAIGA_BASE_URL="https://projects.arbisoft.com"
export TAIGA_AUTH_TOKEN="<taiga_access_token>"

export PLANE_URL="https://plane.arbisoft.com"
export PLANE_UPLOAD_BASE_URL="http://plane-nginx"
export PLANE_WORKSPACE_SLUG="arbisoft"
export PLANE_API_KEY="<plane_access_token>"

export PLANE_DB_HOST="<db_host>"
export PLANE_DB_PORT="<db_port>"
export PLANE_DB_NAME="<db_name>"
export PLANE_DB_USER="<db_user>"
export PLANE_DB_PASSWORD="<db_password>"

export MIGRATE_TASKS_AS_SUBISSUES="true"
export MIGRATE_WIKI_PAGES_VIA_DB="true"
export PLANE_RATE_LIMIT_DELAY="0"
```

## Recommended Structure
```text
migration-scripts/
  layer0_user_presync.py
  taiga_user_sync.py
  layer1_taiga_to_plane.py
  layer2_update_plane_db.py
  README.md
  dumps/
  runs/
```

## Execution Order
1. Layer 0
2. Layer 1
3. Layer 2

## Commands
### Layer 0
```bash
mkdir -p runs/layer0
python3 layer0_user_presync.py \
  --report-prefix runs/layer0/taiga_global_user_presync | tee runs/layer0/layer0.log
```

### Layer 1
```bash
mkdir -p runs/layer1
cd runs/layer1
python3 -u ../../layer1_taiga_to_plane.py \
  ../../dumps/project1.json
```

Output:
- `runs/layer1/project1/migration_mapping_project1.json`
- `runs/layer1/project1/migration_mapping_project1.checkpoint.json`
- `runs/layer1/project1/layer1.log`

Optional multi-dump command:
```bash
mkdir -p runs/layer1
cd runs/layer1
python3 -u ../../layer1_taiga_to_plane.py \
  ../../dumps/project1.json \
  ../../dumps/project2.json
```

Multi-dump behavior:
- each dump runs in isolation
- each dump gets its own folder, mapping, checkpoint, and `layer1.log`
- no shared mapping file is used across dumps

### Layer 2
```bash
mkdir -p runs/layer2/project1
cd runs/layer2/project1
python3 ../../../layer2_update_plane_db.py ../../layer1/project1/migration_mapping_project1.json | tee layer2.log
```

## Rules
- Use a fresh Layer 1 run before Layer 2.
- Do not reuse an old partial mapping/checkpoint set.
- Run Layer 0 first so assignees and user mappings work correctly.
- For large dumps, run all layers on the server.
- **Recommended: run Layer 1 with one dump file at a time for the best performance, easiest recovery, cleaner validation, and lowest risk of operational mistakes.**

## Success Checks
After Layer 2, verify a few representative items for:
- assignee
- attachments opening
- watcher/subscriber sync
- timestamps
- authorship
- page formatting and links
