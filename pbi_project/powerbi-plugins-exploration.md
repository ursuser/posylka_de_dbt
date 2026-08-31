# Power BI Plugins / Skills — Exploration

## Goal

Try out the Microsoft `powerbi-authoring` skill bundle (from `microsoft/skills-for-fabric`) for editing this project's `.pbip` files, and find out in practice where the Mac + Parallels setup blocks it.

---

## Setup — DONE (2026-07-04)

Installed at Claude Code **user level** (not project-level — applies to every project, same as the dbt/BigQuery plugins already in use):

- Marketplace: `microsoft/skills-for-fabric` → registered as `fabric-collection`
- Plugin: `powerbi-authoring@fabric-collection` v0.3.6, scope `user`
- 6 skills installed: `powerbi-report-authoring`, `powerbi-report-design`, `powerbi-report-management`, `powerbi-report-planning`, `semantic-model-authoring`, `check-updates`
- 1 MCP server: `powerbi-modeling-mcp`
- Two global npm CLIs installed (Node 22.12, satisfies the ≥20 requirement):
  - `@microsoft/powerbi-report-authoring-cli` → binary `powerbi-report-author` (v0.1.1)
  - `@microsoft/powerbi-desktop-bridge-cli` → binary `powerbi-desktop` (v0.1.1)

Does **not** require a Microsoft Fabric workspace, Azure AD login, or Power BI Premium — the authoring skill works purely against local `.pbip`/`.Report` JSON files. Fabric itself is Microsoft's cloud analytics platform (roughly: BigQuery + Dataflow + Looker bundled into one SaaS with shared storage/OneLake) — not needed for local Power BI Desktop work, which is what this project uses.

---

## Environment constraint — CONFIRMED

Power BI Desktop runs inside a **Windows VM under Parallels**; the `.pbip` project files themselves live on the **Mac filesystem** (shared folder) and are opened by Desktop from there. Claude Code (and the CLIs it calls) run on the **Mac side**.

Tested `powerbi-desktop status` directly:

```json
{
  "status": "not_connected",
  "instances": [],
  "diagnostics": [
    { "code": "UNSUPPORTED_OS", "message": "Power BI Desktop Bridge discovery is only supported on Windows." }
  ]
}
```

**Conclusion:**
- `powerbi-report-author` (offline: `validate`, `catalog`, `preview-visuals`, `preview-pages`, `preview-filters`, `preview-themes`, `formatting`, `expr`, `theme`, `doctor`) — works fine, pure file I/O against the `.pbip`/`.Report` JSON on disk. No dependency on Parallels or a running Desktop process.
- `powerbi-desktop` (Desktop Bridge: `open`, `reload`, `screenshot`, `screenshot-all`, `manifest`) — **does not work** in this setup. Process discovery of `PBIDesktop.exe` is Windows-only by design, and Claude Code isn't running inside the VM. Would only work if the CLI were installed and run *from inside* the Windows VM itself — not attempted, unclear if worth the setup effort.
- Practical effect: the documented skill workflow "edit → validate → reload → screenshot → verify" collapses to "edit → validate" here. Reload/screenshot review stays manual in Desktop, same as the existing homegrown workflow (see project's own Power BI dashboard-building process: close Desktop before JSON edits, reopen after).

---

## Update — 2026-08-31

The `powerbi-modeling-mcp` MCP server (part of the same `powerbi-authoring` plugin, backs the `semantic-model-authoring` skill) also fails to connect from a Claude Code session running on the Mac side — `CONNECTION_CLOSED`.

Same root cause as the bridge CLI above: it needs a live connection to the Power BI Desktop process, which only exists inside the Parallels Windows VM. Confirms this is a general constraint, not specific to the bridge CLI — any part of the plugin that needs a running Desktop instance is unusable from the Mac side; only the pure file-I/O CLI commands work.

---

## Update — 2026-08-31 (2): live reload confirmed working

The August 2026 Power BI monthly release added a feature separate from the Desktop Bridge: Desktop now watches the open PBIP project's files on disk and shows an "Apply external changes" banner when they change externally, no restart needed. Unlike the Bridge (named pipe, local-only, remote access not supported per Microsoft's own docs), this is Desktop's own file-watcher — same file access path Desktop already uses to open the project, so it isn't blocked by the Mac ↔ Parallels boundary.

**Tested and confirmed working end-to-end:**

- Environment: `posylka_bq.pbip` open in Power BI Desktop inside the Parallels Windows VM; file edited via Claude Code on the Mac side, through the shared folder.
- On Desktop **2.154.956.0 (May 2026)**: no banner appeared — feature not present in that build.
- Updated Desktop via Microsoft Store (Library → Get updates) to the August 2026 build.
- Repeated the same edit (changed a page's `displayName` in `page.json`): banner **"This project's files were changed externally. Apply to view the latest changes."** with an **"Apply external changes"** button appeared immediately. Clicking it updated the page tab name live, no restart.

**Conclusion:** the old "close Desktop before JSON edits, reopen after" workflow (see project's Power BI dashboard-building process) is obsolete on Desktop August 2026+. Updated the global skill (`~/.claude/skills/power-bi-dashboard-building/SKILL.md`) to use the apply-external-changes flow as the default, with close/reopen kept only as a fallback for older Desktop builds.

**Caveat:** while Desktop has the external change pending (banner shown but not yet applied) or has just applied it, don't make further edits to the same files from Claude Code — Desktop holds its own in-memory copy, and a save from Desktop would overwrite anything written to disk in between. Wait for the user to confirm they applied and saved before touching the files again.

---

## To try

- TODO: run `powerbi-report-author validate` against `posylka_bq.pbip` and see what it reports
- TODO: run `powerbi-report-author preview-visuals` / `preview-pages` on an existing page and compare against manually reading the JSON
- TODO: try `powerbi-report-author catalog` / `formatting` to see if visual/formatting metadata lookup beats guessing field names by hand
- TODO: try one of the `powerbi-report-design` / `powerbi-report-planning` / `powerbi-report-management` skills on a real request (e.g. planning a new page) and compare against the existing manual workflow
- TODO: decide whether `semantic-model-authoring` skill adds anything over writing DAX measures by hand
- TODO: if reload/screenshot automation turns out to matter, evaluate installing Node + the bridge CLI inside the Parallels Windows VM directly (separate effort, not started)
