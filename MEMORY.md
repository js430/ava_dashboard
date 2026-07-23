# MEMORY.md
Decision log for ava_dashboard. Read this at the start of every session.

---

## 2026-07-23 — Nav: "Admin Tools" dropdown, click-to-open, duplicated per template

**Decided:** The four admin-only nav links collapse into one `Admin Tools`
dropdown holding **Visitor Log** (renamed from "Admin"), **Contributors**, and
**Network**. Raffle stays a top-level button. Opens on **click**, closes on
outside-click or Esc (`toggleNavGroup` / `closeNavGroups`, ~15 lines appended
before `</body>` in each template).

**Why click, not hover:** hover doesn't exist on touch, the nav already wraps on
phones, and a hover menu sitting next to "Log out" pops open on the way past.
Rejected: pure-CSS `:hover` (cleanest diff, unusable on mobile) and native
`<details>` (free a11y, but fiddly popup positioning and browser-inconsistent).

**Duplicated across 10 templates on purpose.** There is no Jinja base template
in this repo — every page is standalone with its own inlined `<style>`. The
dropdown CSS/markup/JS is therefore copy-pasted into all 10. **If you change the
dropdown, change it in all 10** (`status, card_tracker, map, contributors,
raffle_wheel, invite_network, scan, analytics, admin, index`). Extracting a
shared base template is the real fix; not attempted here.

**Three template quirks that will look like bugs in the diff:**
- `admin.html` and `analytics.html` do **not** wrap the group in
  `{% if is_admin %}` — their routes (`main.py`) never pass `is_admin`, so a
  guard would hide the menu on the pages it belongs to. Matches how their nav
  links were already hardcoded.
- `invite_network.html` has an `{% else %}` branch with a standalone Network
  link: `/invite-network` is viewable by non-admin `all_mods` users, who would
  otherwise lose the link by it moving inside an admin-only group.
- `scan.html` is dark-by-default with a `html[data-theme="light"]` override —
  inverted from every other template, so it needs its own colour block.
- `index.html` uses `.logout`-classed links in `.header-right`, not `.nav`.

**Mobile:** menu is `right: 0` anchored (nav sits at the right edge on desktop);
each template's media query flips it to `left: 0` so a wrapped nav row doesn't
push the 150px panel off-screen left.

---

## 2026-07-23 — Card tracker: detail panel above the table, 7/14/30-day graph range

**Decided:** The card detail panel moved above the main table (was below), and
the price graph got a 7/14/30-day range toggle defaulting to 30.

**Range filtering is client-side.** `/api/card-tracker/history` already returns
every snapshot for a card in one response, so switching range trims the array
already in memory — no extra request, no JustTCG budget. Rejected adding a
`days` query param: it would spend a round-trip per toggle for data already
sent.

**The window is anchored to the card's newest snapshot, not to `now`.** A card
whose prices haven't refreshed in weeks still draws a line on the 7-day view
instead of going blank. Trade-off: the "last 7 days" label is relative to the
data, not the calendar — the note under the graph says which.

**Score and momentum chips still use full history** regardless of range; they're
computed server-side in `card_scoring.score_card`. The explainer text under the
graph says so explicitly, so nobody reads a 7-day graph and expects the 30d
momentum number to track it.

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
