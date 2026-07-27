# MEMORY.md
Decision log for ava_dashboard. Read this at the start of every session.

---

## 2026-07-26 — Card catalog: local cache filled free off the grading calculator

**Decided:** New `/catalog` page — browse/filter every card the dashboard has
seen by set, rarity and raw market price. Backed by a new dashboard-owned
table `catalog_cards` (`catalog.py`, `CREATE TABLE IF NOT EXISTS` at startup,
same convention as the card tracker). Reads never touch a vendor.

**The finding that shaped the whole design:** PPT's `/cards` response already
carries `prices.market` and `rarity` for **every** card, and
`price_sources.fetch_ppt_set_cards` was already paying for that call (billed
per card) to drive the grading calculator's picker — then discarding the
price. `fetch_pokemonpricetracker` reads raw from exactly that field. So a
whole set's raw prices cost **zero extra credits**; they were being thrown
away. `_ppt_raw_price()` was added and the price threaded onto the existing
card dicts (additive — the picker ignores the new key).

**Filling is lazy, never member-triggered.** `/api/grading-calculator/set-cards`
upserts its result into `catalog_cards` on the way past, so browsing the
calculator stocks the catalog for free. Plus an admin-only `POST
/api/catalog/stock` for seeding sets deliberately. **A member browsing
/catalog can never cause a vendor call** — `/api/catalog/cards` is a pure DB
read. That's the point: "every holo under $50 across all sets" spans ~36,000
cards, and at 1 credit/card a full upfront backfill is unaffordable. The
catalog stocks itself from real usage instead.

**Seeded with the newest 20 sets** (`CATALOG_BACKFILL_SETS`, env-tunable, 0
disables) by a background task at startup, so the page isn't empty on day one.
Everything older arrives lazily.

**The seed slices BEFORE filtering out already-stocked sets, and that order is
load-bearing.** `catalog.select_backfill_window()` takes the newest N *then*
drops what's cached. The obvious alternative — "the next N sets nobody has
stocked" — walks further back through the catalog on every call, and since
Railway redeploys on every push and the in-process PPT cache dies with the
process, that would re-spend ~4,000 credits on **every deploy, forever**.
Slicing first pins a fixed window: first boot pays once, every later boot is a
no-op, and a newly released set enters the window on its own. This is covered
by tests (the redeploy no-op, the new-release case, and that a set pushed out
of the bottom of the window is never re-fetched) — if you change that
function, keep those properties.

**The seed stops after 3 consecutive empty sets.** `fetch_ppt_set_cards`
returns `[]` for a credit/rate limit exactly as it does for a genuinely empty
set, so consecutive empties are the only available signal that the budget ran
out. Better to stop than grind through 20 futile sets. If PPT ever
distinguishes these, prefer the real signal.

**Rejected:** a full nightly sync of all ~180 sets (blows the credit budget —
verify the actual quota in the `X-RateLimit-Daily-Remaining` log line before
anyone revisits this); on-demand fetching from the catalog page (a member
typing in a price filter would spend credits); client-side filtering (can't
answer cross-set questions without holding every set in the browser).

**Pokémon-only on purpose.** One Piece's catalog source (optcgapi) returns
rarity but **no prices at all**, so OP cards would sit in a price-filtered
table with nothing to filter on. `CATALOG_GAME = "pokemon"` in `main.py`; the
table's `game` CHECK still permits `one_piece` so adding it later is a code
change, not a migration.

**Two schema details that will look wrong if you skim them:**
- `rarity` is part of the unique index. One Piece prints alt-art/manga
  variants under the *same name and number* — without rarity in the key those
  collapse into one row and a variant silently vanishes. Costs nothing for
  Pokémon; correct for when OP is wired up.
- A refresh takes the new response as truth: a card that no longer reports a
  price has its cached price **cleared**, not left to look current.
  `priced_at` is set only when a price is actually stored, so it always
  describes the number next to it.

**Freshness is honest, not live.** Prices are captured when a set was last
stocked. The page says so, and a blank price renders as "no price", never
`$0` — those mean different things and conflating them would misprice a card.

**Security:** `ORDER BY` resolves through the `SORT_COLUMNS` allowlist
(unknown keys fall back to the default) — same rule as the raffle wheel's
`_pick_col`. Everything else is parameterized, including ILIKE patterns,
whose `%`/`_` are escaped so typed text can't become a wildcard. Page and
read APIs are member-gated; `/api/catalog/stock` is `require_admin` because
it spends credits.

**Nav:** this is an **11th** template carrying the copy-pasted nav dropdown
(see the 2026-07-23 entry). The `Card Catalog` link was added to all 10
existing templates in the same change, per that entry's rule. Extracting a
shared base template is still the real fix and still wasn't attempted.

**Cross-repo:** additive only — one new dashboard-owned table, no bot-owned
table touched. Log in data-system.md's ownership table.

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

## 2026-07-23 — Card tracker: detail panel above the table, 7/14/30/90-day graph range

**Decided:** The card detail panel moved above the main table (was below), and
the price graph got a 7/14/30/90-day range toggle defaulting to 30.

**90 days is pursued two ways, on the assumption neither is reliable alone.**

*Ask the source for more.* JustTCG's `priceHistoryDuration` accepts
7d/30d/90d/180d/1y and **defaults to 7d** — so a request that fails to carry the
parameter silently returns a week, which is what we were seeing. Fixed in
`justtcg.py`: duration is now `HISTORY_DURATION` (env `JUSTTCG_HISTORY_DURATION`,
default `90d`) and history is requested **three ways at once** —
`priceHistoryDuration` and `include_price_history` on the query string, plus
`include_price_history: true` on each batch body item (the field their SDK
documents on `BatchLookupItem`). The query string alone appears to be ignored on
the batch POST. Unverified against a live key — `_log_history_depth()` logs the
returned point count every run precisely so a regression to ~7 is visible
instead of silent.

*Accumulate regardless.* **Nothing has ever deleted snapshots** — the only
`DELETE FROM price_snapshots` statements are the two admin buttons — so each
nightly ingest adds a day and the window fills in over ~3 months even if the
source only ever hands back a week. Deliberately did NOT add a retention/prune
job; rows older than 90 days are kept.

**Backfill now runs every ingest, not just on a card's first fetch.** It inserts
only calendar days (UTC) the card doesn't already have, so it is idempotent
without a unique index on (card_id, day) — and it self-heals two things the old
first-fetch-only version could not: nights the scheduler didn't fire, and cards
permanently stuck on a 7-day history from before this fix. Costs no extra API
calls (same requests, bigger payload). Rejected adding a unique constraint:
existing rows may already contain same-day duplicates from manual refreshes, so
the migration could fail on live data.

**Graph collapses to one point per calendar day** (`dailySeries`). A manual
"Refresh prices" can add several snapshots to a day on top of the scheduled
nightly one, which would otherwise show as a jagged multi-point day. Last
snapshot of a day wins.

**Range windows step by calendar days, not `n * 86400000`.** A fixed 24h step
drops a point from the window across a DST change — caught by a test, fixed with
`setDate()`. Same trap applies to any future date-window code here.

**Gaps are surfaced, not hidden.** The note under the graph counts days in the
span with no price ("5 days in that stretch have no price yet") so a nightly
ingest that stopped landing is visible instead of looking like a smooth line.
The scheduler is an in-process asyncio loop (`main.py`), so a Railway restart
near 11pm silently skips that day. No startup catch-up was added on purpose —
Railway redeploys on every push and each catch-up would spend JustTCG's 100/day
free-tier budget.

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
