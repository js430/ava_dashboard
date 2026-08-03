# CLAUDE.md

## Who I Am
Jeffrey Shi. This repo is **ava_dashboard** — the companion web dashboard for `ava_bot`, the Discord restock community covering NOVA, RVA, Tidewater, and the wider DMV (Virginia, Maryland, DC). The dashboard gives members historical restock data, analytics, store maps, a TCG card scanner with price lookups, and an admin-only raffle wheel. Public URL: https://www.nexuscardco.com/.

## Tech Stack
- Python, **FastAPI + uvicorn** — async routes, Jinja2 templates in `templates/`
- **PostgreSQL via asyncpg** — pool on `app.state.db`, parameterized `$1 $2` queries
- **Auth:** Discord OAuth2, role-gated (`REQUIRED_ROLE_ID` / `DENY_ROLE_IDS`); stateless HMAC-signed OAuth `state` (no session cookie needed for the redirect round-trip)
- **slowapi** rate limiting keyed on the real client IP behind Railway's proxy (`X-Real-IP`)
- **Anthropic API** (`claude-sonnet-4-6`) for the card scanner; external price APIs: pokemontcg.io, Scryfall, YGOProDeck, optcgapi
- Deployed on **Railway** — Dockerfile build, CI/CD auto-deploys on push
- GitHub repo: js430/ava_dashboard

## Shared Database — read the contract FIRST
This dashboard shares **one PostgreSQL database with `ava_bot`**. The bot writes; this app mostly reads. Before changing a query or relying on a table, read the cross-repo contract: `E:\Obsidian\claude brain\Reference\data-system.md`.

- **This app writes:** `terms_acceptance`, `dashboard_sessions`, `user_preferences` — but no `CREATE TABLE` exists in either repo; treat their schema as live-DB-only until confirmed.
- **This app reads bot-owned tables:** `restock_reports`, `command_logs`, `users`, `member_joins`, `hope_contributions`, plus the unverified `locations`, `plusones`, `manual_points`, `active_informants`.
- A bot-side schema change can break this app silently. **Never assume a column exists** — verify against the live DB (`pg_dump --schema-only`) or the bot's `cogs/database.py`.

## Communication Style
- Never open with filler ("Great question!", "Certainly!", etc.). Start with the answer.
- Match response length to task complexity. No padding, no restating the question.
- Don't ask clarifying questions unless something is genuinely ambiguous. Use judgment.
- Plain English in all user-facing text — no technical jargon in the UI, page copy, or error messages.
- If uncertain about any fact, statistic, date, or technical detail: say so explicitly before including it. Never fill gaps with plausible-sounding information.

## Default Behaviors Every Session
**Before any significant task:** Show 2-3 approaches with tradeoffs. Wait for a choice before proceeding.

**Before writing code:** Always read the relevant files first. `main.py` is large — read the actual route/handler, never assume structure from names alone.

**Scope discipline:**
- Only modify files, functions, and lines directly related to the task. Don't refactor, rename, or reformat anything not asked for.
- If something elsewhere is worth fixing, note it at the end. Don't touch it.
- Before any change that significantly alters existing behavior (rewriting a route, changing auth/session logic, restructuring a template): stop, describe exactly what will change and why, wait for confirmation.
- Before deleting any file, dropping DB records, or removing dependencies: stop, list exactly what's affected, ask for explicit confirmation. "You mentioned this earlier" is not confirmation.

**Web & security rules (this app is publicly exposed):**
- Don't weaken the existing security posture without flagging it loudly: CSP and security headers, HMAC OAuth `state`, real-IP rate limiting, role gating, session cookie flags (`https_only`, `same_site`). These are deliberate.
- Admin routes must keep the `ADMIN_USER_IDS` check. Never expose admin data on member-accessible routes.
- Any table/column name resolved at runtime must stay allowlisted before being interpolated into SQL (see the raffle-wheel `_pick_col` / `_find_table` pattern). Everything else uses parameterized queries.

**Git workflow:**
- Commit and push together when asked. Never one without the other unless told otherwise.
- Push with `git push origin HEAD` (current branch, not main).
- Commit messages: descriptive subject, bullet body for multi-part changes, always end with `Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>`.
- Note: this repo's remote is SSH (`git@github.com:js430/ava_dashboard.git`).

## bot-development Pipeline (planning vault)
Feature work is tracked in the Obsidian vault at `E:\Obsidian\claude brain\bot-development\` (`Inputs → Process → Outputs → Feedback`). Many features span this repo and `ava_bot` — if a `Process/` spec touches shared data, treat both repos as one change (see the data-system contract above). Launch with `--add-dir "E:\Obsidian\claude brain\bot-development"` to read specs while editing here.

**Hard gate — shipping = communicated:** No PR is "done" until a plain-language patch note exists (*what changed, why a member cares, how to use it*). That's the GitHub Release text and the Discord announcement source; mirror it into the vault `Outputs/` folder.

## MEMORY.md
- Maintain `MEMORY.md` in this repo as a decision log.
- After any significant decision, add an entry: what was decided, why, what was rejected and why.
- Read MEMORY.md at the start of every session.
- Never contradict a logged decision without flagging it first.
- A decision that also affects `ava_bot` (shared DB/schema) gets logged in one repo and linked from the other — not duplicated.
