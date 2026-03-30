# Dagster + dbt Orchestration Setup

**Status:** DONE — orchestration is live and tested.

**Goal:** Set up scheduled dbt model runs via Dagster running in Docker on remote server.

**Why:** dbt Cloud is not available for running jobs; need a self-hosted orchestration solution.

**Remote server:** `root@91.107.197.188` (hostname: `dbt-orchestration`)

## Server Structure

The server hosts multiple projects on a single Dagster instance using **code locations**.
Each project is a git clone of its GitHub repo. `dagster_project/` lives inside the dbt repo.

```text
/root/
├── orchestration/                  # Dagster + dbt (main purpose of the server)
│   ├── docker-compose.yml          # 3 containers: webserver, daemon, postgres
│   ├── workspace.yaml              # lists code locations (one per project)
│   ├── dagster_home/
│   │   └── dagster.yaml            # Dagster config (Postgres storage)
│   └── projects/
│       ├── fittra_uk/
│       │   └── dbt_project/        # git clone, dbt + dagster_project/ inside
│       └── posylka_de/
│           └── dbt_project/        # git clone from github.com/ursuser/posylka_de_dbt
│               ├── models/
│               ├── dagster_project/
│               │   ├── __init__.py
│               │   ├── assets/dbt_assets.py
│               │   ├── resources/dbt_resource.py
│               │   ├── resources/__init__.py        # email helper
│               │   ├── resources/email_on_failure.py
│               │   └── schedules/daily_schedule.py
│               └── dbt_project.yml
│
└── n8n/                            # n8n (separate, not running by default)
    ├── docker-compose.yml          # start: cd /root/n8n && docker compose up -d
    └── data/
```

## Docker Containers

| Container | Purpose |
|-----------|---------|
| `orch-postgres` | PostgreSQL — shared storage for Dagster |
| `orch-dagster-webserver` | UI on port 3000, installs git + dbt + dagster on start |
| `orch-dagster-daemon` | Background process for schedules and sensors |

## How It Works

- `docker-compose.yml` describes which containers to run
- dbt projects contain the SQL models (source code)
- `dagster_project/` inside each dbt repo defines what to run and when (assets, schedules)
- `workspace.yaml` tells Dagster where to find each code location
- `dagster_home/` stores logs and run history
- Dagster UI: `http://91.107.197.188:3000`
- To view dbt logs (bytes processed, rows, timing): Runs → select run → View → select step → stdout/stderr

## Key Config Files on Server

**`/root/.dbt/profiles.yml`** — dbt profiles for both projects:
- `fittra_uk_dbt`: BigQuery project `fittra-dbt`, dataset `dbt_prod`, location `US`
- `posylka_de_dbt`: BigQuery project `posylka-dbt`, dataset `dbt_prod`, location `EU`, keyfile `posylka-dbt-98d36ba43645.json`

**`/root/.credentials/`** — BigQuery service account keys (mounted read-only into containers)

**`/root/orchestration/workspace.yaml`** — two code locations: `fittra_uk`, `posylka_de`

## Git-Based Deployment

- dbt project on server is a `git clone` from GitHub (HTTPS + PAT)
- PAT: `dagster-server-dbt-read` (fine-grained, Contents: Read-only, expires end of 2026)
- Repo: `https://github.com/ursuser/posylka_de_dbt.git` (private)
- Workflow: edit locally → commit → push → `git pull` on server → restart Dagster
- `dagster_project/` is committed to git alongside dbt models

## Posylka DE Jobs & Schedules

| Name | Type | What | When |
|------|------|------|------|
| `posylka_de_daily` | scheduled job | models tagged `daily` | every day 05:00 UTC |
| `posylka_de_weekly` | scheduled job | models tagged `weekly` | every sunday 18:00 UTC |
| `posylka_de_run_all` | manual job | all models | on demand from UI |
| `daily_schedule` | schedule | triggers `posylka_de_daily` | cron `0 5 * * *` |
| `weekly_schedule` | schedule | triggers `posylka_de_weekly` | cron `0 18 * * 0` |
| `email_on_run_failure` | sensor | sends email on any job failure | automatic |

Email alerts go to `ursuser@gmail.com` (Gmail App Password stored in `ALERT_EMAIL_PASSWORD` env var in docker-compose).

## How to Update dbt Models

1. Edit models locally
2. `git commit` + `git push` (push done manually — SSH key requires password)
3. SSH to server: `ssh root@91.107.197.188`
4. Pull: `cd /root/orchestration/projects/posylka_de/dbt_project && git pull`
5. If dagster_project changed: `cd /root/orchestration && docker compose restart dagster-webserver dagster-daemon`
6. If only dbt models changed: no restart needed, next run picks up changes

**If new dbt models were added** (new assets not showing in Dagster UI):
- Recompile manifest inside container: `docker exec orch-dagster-webserver bash -c 'cd /app/projects/posylka_de/dbt_project && dbt compile --profiles-dir /root/.dbt --target prod'`
- Then restart: `cd /root/orchestration && docker compose restart dagster-webserver dagster-daemon`

## How to Manage Server

```bash
# SSH access (load key first: ssh-add ~/.ssh/id_rsa)
ssh root@91.107.197.188

# Start/stop all
cd /root/orchestration && docker compose up -d
cd /root/orchestration && docker compose down

# Restart Dagster only
cd /root/orchestration && docker compose restart dagster-webserver dagster-daemon

# View container stats
docker stats --no-stream

# View logs
docker logs --tail 30 orch-dagster-webserver
docker logs --tail 30 orch-dagster-daemon

# Start n8n (when needed)
cd /root/n8n && docker compose up -d
```

## Completed Steps

- [x] SSH access verified
- [x] Renamed server: `fittra-analytics` → `dbt-orchestration`
- [x] Renamed folder: `/root/fittra/` → `/root/orchestration/`
- [x] Renamed containers: `fittra-*` → `orch-*`
- [x] Separated n8n into `/root/n8n/`
- [x] Restructured for multi-project (projects/ subfolder)
- [x] Deployed Posylka DE dbt project via git clone
- [x] Uploaded BigQuery service account key
- [x] Added `posylka_de_dbt` profile (location: EU)
- [x] Created `dbt_prod` dataset in BigQuery (EU)
- [x] Created dagster_project in git repo (assets, resources, schedules)
- [x] Added dagster-daemon container for schedules
- [x] Updated dagster.yaml to use Postgres storage
- [x] Configured workspace.yaml with two code locations
- [x] Set up email alerts on failure (Gmail SMTP)
- [x] Enabled schedule + sensor in Dagster UI
- [x] First successful `posylka_de_run_all` build (54 seconds)
- [x] Fixed BQ project: `posylka-de-478903` → `posylka-dbt` (correct project for dbt)
- [x] Fixed BQ service account key: `posylka-dbt-98d36ba43645.json`
- [x] Set `ga4_start_date: '20260201'` in `dbt_project.yml` for prod
- [x] Full-refresh build on prod with data from Feb 1 — all 25 steps PASS
- [x] Server synced with latest git (`git pull` done)

## TODO

- [x] Verify daily schedule triggers correctly at 05:00 UTC — fixed: schedule was not enabled, added `default_status=RUNNING` (2026-03-14)
- [x] Add `git pull` automation before each dbt build — added `subprocess.run(["git", "pull"])` in `dbt_assets.py` (2026-03-14)
- [x] Fix `int_gclid_campaign` MERGE duplicate error — added `qualify row_number()` dedup (2026-03-14)
- [ ] Verify daily schedule runs successfully at 05:00 UTC (check 2026-03-15)
