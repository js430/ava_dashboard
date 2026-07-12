# MEMORY.md
Decision log for ava_dashboard. Read this at the start of every session.

---

## 2026-07-11 — Card tracker (trial): dashboard-owned tables, startup-ensure DDL

**Decided:** The card profit-potential tracker's three tables (`tracked_cards`,
`price_snapshots`, `card_scores`) are **dashboard-owned** and created via
`CREATE TABLE IF NOT EXISTS` at app startup (`card_tracker.py`, called from the
lifespan) and by the ingest script. Watchlist lives in `watchlist.py` (config,
not DB). Page access reuses the existing `MOD_ROLE_IDS` session-mod flag +
admins — regional mods get access by adding their role ID to `MOD_ROLE_IDS`
in Railway (comma-separated), no code change.

**Why:** Neither repo has a migration system; startup-ensure matches ava_bot's
own convention and finally puts dashboard-owned DDL in code (the older gap
flagged 2026-06-21 still stands for the three auth tables). Env-var gating
avoids hardcoding a role ID that only Railway knows.

**Rejected:** A manual one-shot migration script (fresh deploys would 500 until
run); a separate TRACKER_ROLE_IDS env (more knobs than the trial warrants —
revisit if mod-tier access ever needs to diverge from sample-preview access).

**Cross-repo note:** additive only — new tables, no changes to bot-owned
tables. Logged in data-system.md ownership table.

---

## 2026-06-21 — Established the shared-DB cross-repo contract

**Decided:** Documented that `ava_dashboard` and `ava_bot` share one Railway PostgreSQL database, with the contract written up at `E:\Obsidian\claude brain\Reference\data-system.md`. The bot owns/writes the data tables; this app mostly reads them and owns only `terms_acceptance`, `dashboard_sessions`, `user_preferences`.

**Why:** A schema change in the bot (rename/drop/retype a column) can break this app silently in production, because nothing in the bot's path tests the dashboard. The contract makes that coupling explicit.

**Found during setup:** Several tables this app depends on — `locations`, `plusones`, `manual_points`, `active_informants`, plus the three this app writes — have **no `CREATE TABLE` in either repo**. Their schema lives only in the live DB. `locations` is the highest-risk (maps + restock joins depend on it). TODO: run `pg_dump --schema-only` and backfill these definitions into whichever repo should own them.

**Rule going forward:** Additive changes (nullable column, new table) are safe. Destructive changes to a shared table require grepping `ava_dashboard/main.py` for the table/column first, and updating both repos in the same change.

---
