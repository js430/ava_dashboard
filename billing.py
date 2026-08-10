"""Non-Discord accounts + Stripe subscriptions — schema, tokens, entitlement.

Owned by ava_dashboard (see data-system.md): acct_accounts, acct_login_tokens,
acct_subscriptions. Nothing here is shared with ava_bot.

WHAT A PAID ACCOUNT GETS: the Nexus Playground tools only (Grading Calculator,
Card Catalog) at full access — i.e. what a guest sees, minus the guest
restrictions. NOT the restock dashboard, map, analytics, card tracker or
inventory: those stay Discord-members-only.

THE PROPERTY THAT MAKES THAT SAFE, and the one to preserve if you touch this:
a paid account NEVER gets `session["user"]`. It gets `session["account_id"]`.
Every Discord-gated route reads `session["user"]` and redirects/401s without
it, so all of them stay closed by CONSTRUCTION rather than by anyone
remembering to add a check. Granting a local account a `session["user"]` dict
would silently open the entire dashboard.

IDENTITY: acct_accounts.id is a SERIAL reused directly as the BIGINT user_id
in the existing per-user tables. That's safe because Discord snowflakes are
~1e16 and up (they encode ms since 2015 shifted left 22), so a serial starting
at 1 cannot collide. The CHECK constraint below pins that assumption in the
schema instead of leaving it as folklore.

STRIPE IS THE SOURCE OF TRUTH for entitlement; acct_subscriptions is a mirror
of it, written only by the signature-verified webhook. Never the reverse, and
never trust the client: a browser saying "I subscribed" means nothing until
Stripe says so.
"""

import os
import hmac
import hashlib
import logging
import secrets
from datetime import datetime, timedelta, timezone

logger = logging.getLogger("dashboard.billing")

# Magic-link lifetime. Short on purpose: the token arrives in an email, may sit
# in a shared inbox, and is enough to become the account on its own.
LOGIN_TOKEN_TTL_MINUTES = int(os.getenv("LOGIN_TOKEN_TTL_MINUTES", "15"))
# Bytes of entropy in the emailed token (URL-safe base64 of this length).
LOGIN_TOKEN_BYTES = 32

# Ceiling for a local account id, enforced in the schema. Discord's smallest
# real snowflakes are ~1e16; 1e15 leaves an order of magnitude of headroom and
# would still take longer than the project's lifetime to reach.
MAX_LOCAL_ACCOUNT_ID = 1_000_000_000_000_000

# Stripe subscription statuses that entitle access. 'trialing' is included —
# that IS the trial. Everything else (past_due, canceled, unpaid, incomplete,
# paused) does not entitle: past_due in particular is a FAILED payment, and
# treating it as active is how people get months of free service.
ENTITLING_STATUSES = ("trialing", "active")

ACCOUNT_SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS acct_accounts (
        id                 BIGSERIAL PRIMARY KEY,
        email              TEXT NOT NULL,
        email_lower        TEXT NOT NULL UNIQUE,
        stripe_customer_id TEXT UNIQUE,
        created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_login_at      TIMESTAMPTZ,
        disabled           BOOLEAN NOT NULL DEFAULT FALSE,
        CONSTRAINT acct_id_below_discord_snowflakes
            CHECK (id < 1000000000000000)
    )
    """,
    # email_lower is the unique key, not email: addresses are case-insensitive
    # in practice, and without this "A@x.com" and "a@x.com" become two accounts
    # with two subscriptions.
    """
    CREATE TABLE IF NOT EXISTS acct_login_tokens (
        id          BIGSERIAL PRIMARY KEY,
        account_id  BIGINT NOT NULL REFERENCES acct_accounts(id) ON DELETE CASCADE,
        token_hash  TEXT NOT NULL UNIQUE,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at  TIMESTAMPTZ NOT NULL,
        used_at     TIMESTAMPTZ,
        request_ip  TEXT NOT NULL DEFAULT ''
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_acct_tokens_account ON acct_login_tokens (account_id, created_at)",
    """
    CREATE TABLE IF NOT EXISTS acct_subscriptions (
        stripe_subscription_id TEXT PRIMARY KEY,
        account_id             BIGINT NOT NULL REFERENCES acct_accounts(id) ON DELETE CASCADE,
        status                 TEXT NOT NULL,
        price_id               TEXT NOT NULL DEFAULT '',
        current_period_end     TIMESTAMPTZ,
        trial_end              TIMESTAMPTZ,
        cancel_at_period_end   BOOLEAN NOT NULL DEFAULT FALSE,
        updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_acct_subs_account ON acct_subscriptions (account_id)",
]


async def ensure_billing_schema(pool) -> None:
    """Create the account/billing tables if absent (idempotent)."""
    async with pool.acquire() as conn:
        for ddl in ACCOUNT_SCHEMA:
            await conn.execute(ddl)
    logger.info("Billing schema ensured")


# ── Email + tokens ───────────────────────────────────────────────────────

def normalize_email(raw: str) -> str:
    """Trimmed, lowercased. Deliberately NOT doing gmail dot/plus stripping —
    that would merge addresses their owner considers distinct, and the only
    thing it'd buy is marginally harder trial abuse, which the card
    requirement already handles."""
    return (raw or "").strip().lower()


def looks_like_email(value: str) -> bool:
    """Deliberately loose. A strict RFC regex rejects valid addresses and
    gives a false sense of validation — the real proof the address works is
    that the magic link arrives, so this only rejects the obviously wrong."""
    v = (value or "").strip()
    if len(v) < 3 or len(v) > 254 or " " in v:
        return False
    local, sep, domain = v.partition("@")
    return bool(sep) and bool(local) and "." in domain and not domain.startswith(".")


def new_login_token() -> tuple:
    """(plaintext, hash). The plaintext is emailed and never stored.

    Same reasoning as password hashing: the DB stores only the hash, so a
    leaked table can't be used to log in as anyone. SHA-256 (not bcrypt) is
    appropriate here — the input is 32 bytes of CSPRNG output, so there's no
    guessable low-entropy secret for a slow hash to protect.
    """
    plaintext = secrets.token_urlsafe(LOGIN_TOKEN_BYTES)
    return plaintext, hash_login_token(plaintext)


def hash_login_token(plaintext: str) -> str:
    return hashlib.sha256((plaintext or "").encode("utf-8")).hexdigest()


def token_expiry(now=None) -> datetime:
    now = now or datetime.now(timezone.utc)
    return now + timedelta(minutes=LOGIN_TOKEN_TTL_MINUTES)


def token_is_valid(row, now=None) -> bool:
    """A token row is usable only if it exists, is unused, and is unexpired.

    Single-use matters as much as expiry: a magic link sits in an inbox
    forever, and inboxes get forwarded, synced and breached. Consuming it on
    first use means a leaked email is worth nothing after the real login.
    """
    if not row:
        return False
    if row.get("used_at") is not None:
        return False
    expires = row.get("expires_at")
    if expires is None:
        return False
    now = now or datetime.now(timezone.utc)
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=timezone.utc)
    return expires > now


# ── Entitlement ──────────────────────────────────────────────────────────

def subscription_entitles(sub, now=None) -> bool:
    """True if this subscription row grants access right now.

    Two independent conditions, both required:
      * status is trialing/active — a canceled or past_due sub never entitles
      * the paid-for period hasn't elapsed

    The period check is what covers a missed webhook: if Stripe's
    `customer.subscription.deleted` never arrives (network, downtime, a
    misconfigured endpoint), the mirrored row still expires on its own rather
    than granting access forever. Fail-closed on our side without depending on
    a delivery we don't control.
    """
    if not sub:
        return False
    if sub.get("status") not in ENTITLING_STATUSES:
        return False
    end = sub.get("current_period_end")
    if end is None:
        # No period end recorded (shouldn't happen for a real Stripe sub) —
        # trust the status rather than lock a paying customer out.
        return True
    now = now or datetime.now(timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    return end > now


async def account_is_entitled(pool, account_id: int, now=None) -> bool:
    """Whether an account currently has a paid/trialing subscription."""
    if not account_id:
        return False
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT status, current_period_end FROM acct_subscriptions "
            "WHERE account_id = $1", account_id)
        disabled = await conn.fetchval(
            "SELECT disabled FROM acct_accounts WHERE id = $1", account_id)
    if disabled:
        return False
    return any(subscription_entitles(dict(r), now) for r in rows)


async def get_or_create_account(pool, email: str) -> dict:
    """Look up (or create) an account by email. Returns {id, email}."""
    lower = normalize_email(email)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, email FROM acct_accounts WHERE email_lower = $1", lower)
        if row:
            return {"id": row["id"], "email": row["email"]}
        row = await conn.fetchrow(
            "INSERT INTO acct_accounts (email, email_lower) VALUES ($1, $2) "
            "ON CONFLICT (email_lower) DO UPDATE SET email = acct_accounts.email "
            "RETURNING id, email",
            (email or "").strip(), lower)
        logger.info("Billing: created account %s", row["id"])
        return {"id": row["id"], "email": row["email"]}


async def issue_login_token(pool, account_id: int, request_ip: str = "") -> str:
    """Create a single-use login token, store its hash, return the plaintext.

    Any older unused tokens for this account are invalidated first: requesting
    a new link should retire the previous one, so a forwarded or intercepted
    older email stops working the moment the real user asks for another.
    """
    plaintext, digest = new_login_token()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "UPDATE acct_login_tokens SET used_at = NOW() "
                "WHERE account_id = $1 AND used_at IS NULL", account_id)
            await conn.execute(
                "INSERT INTO acct_login_tokens (account_id, token_hash, expires_at, request_ip) "
                "VALUES ($1, $2, $3, $4)",
                account_id, digest, token_expiry(), (request_ip or "")[:64])
    return plaintext


async def consume_login_token(pool, plaintext: str):
    """Validate and burn a login token. Returns the account dict, or None.

    The UPDATE ... WHERE used_at IS NULL RETURNING is what makes consumption
    atomic: two simultaneous requests with the same token can't both win,
    because only one UPDATE can match the still-null used_at.
    """
    digest = hash_login_token(plaintext or "")
    if not plaintext:
        return None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT account_id, expires_at, used_at FROM acct_login_tokens "
            "WHERE token_hash = $1", digest)
        if not token_is_valid(dict(row) if row else None):
            return None
        claimed = await conn.fetchrow(
            "UPDATE acct_login_tokens SET used_at = NOW() "
            "WHERE token_hash = $1 AND used_at IS NULL RETURNING account_id",
            digest)
        if not claimed:
            return None          # lost the race; someone else burned it
        acct = await conn.fetchrow(
            "SELECT id, email, disabled FROM acct_accounts WHERE id = $1",
            claimed["account_id"])
        if not acct or acct["disabled"]:
            return None
        await conn.execute(
            "UPDATE acct_accounts SET last_login_at = NOW() WHERE id = $1", acct["id"])
    return {"id": acct["id"], "email": acct["email"]}


# ── Stripe webhook -> mirrored subscription ──────────────────────────────

def _ts(value):
    """Stripe sends unix seconds; None stays None."""
    if not value:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def subscription_fields(sub_object: dict) -> dict:
    """Flatten a Stripe subscription object into our mirror columns.

    Pure so the mapping can be tested against real Stripe payload shapes
    without a DB or network.
    """
    price_id = ""
    try:
        items = ((sub_object.get("items") or {}).get("data") or [])
        if items:
            price_id = ((items[0] or {}).get("price") or {}).get("id") or ""
    except (AttributeError, IndexError, TypeError):
        price_id = ""
    return {
        "stripe_subscription_id": sub_object.get("id") or "",
        "customer_id": sub_object.get("customer") or "",
        "status": sub_object.get("status") or "",
        "price_id": price_id,
        "current_period_end": _ts(sub_object.get("current_period_end")),
        "trial_end": _ts(sub_object.get("trial_end")),
        "cancel_at_period_end": bool(sub_object.get("cancel_at_period_end")),
    }


async def apply_subscription(pool, fields: dict) -> bool:
    """Upsert a mirrored subscription, matched to an account by customer id.

    Returns False when the customer isn't linked to an account — which is
    logged and ignored rather than guessed at. Inventing an account from a
    webhook would let anyone with the endpoint URL mint entitlements.
    """
    sub_id = fields.get("stripe_subscription_id")
    customer_id = fields.get("customer_id")
    if not sub_id or not customer_id:
        return False
    async with pool.acquire() as conn:
        account_id = await conn.fetchval(
            "SELECT id FROM acct_accounts WHERE stripe_customer_id = $1", customer_id)
        if not account_id:
            logger.warning("Billing: subscription %s for unknown customer %s — ignored",
                           sub_id, customer_id)
            return False
        await conn.execute(
            """
            INSERT INTO acct_subscriptions (stripe_subscription_id, account_id, status,
                price_id, current_period_end, trial_end, cancel_at_period_end, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
            ON CONFLICT (stripe_subscription_id) DO UPDATE SET
                status               = EXCLUDED.status,
                price_id             = EXCLUDED.price_id,
                current_period_end   = EXCLUDED.current_period_end,
                trial_end            = EXCLUDED.trial_end,
                cancel_at_period_end = EXCLUDED.cancel_at_period_end,
                updated_at           = NOW()
            """,
            sub_id, account_id, fields["status"], fields["price_id"],
            fields["current_period_end"], fields["trial_end"],
            fields["cancel_at_period_end"])
    logger.info("Billing: subscription %s -> %s (account %s)",
                sub_id, fields["status"], account_id)
    return True


async def link_customer(pool, account_id: int, customer_id: str) -> None:
    """Attach a Stripe customer id to an account, so later webhooks resolve."""
    if not account_id or not customer_id:
        return
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE acct_accounts SET stripe_customer_id = $1 WHERE id = $2",
            customer_id, account_id)


def stripe_configured() -> bool:
    return bool(os.getenv("STRIPE_SECRET_KEY") and os.getenv("STRIPE_PRICE_ID"))


def webhook_configured() -> bool:
    return bool(os.getenv("STRIPE_WEBHOOK_SECRET"))


# ── Webhook signature ────────────────────────────────────────────────────
# The webhook endpoint is exempt from this app's CSRF middleware (Stripe can't
# send our token), so THIS is the only thing standing between a stranger with
# the URL and a free subscription. It is written by hand rather than pulled
# from the SDK for one reason: hand-rolled means it can be tested exhaustively
# here — valid, wrong-secret, tampered-body, replayed, malformed — whereas an
# unrunnable import could only be trusted.
#
# Scheme (Stripe's documented v1): the signed payload is "<timestamp>.<body>",
# HMAC-SHA256 with the endpoint secret, hex-encoded, delivered as
#   Stripe-Signature: t=<unix>,v1=<hex>[,v1=<hex during key rotation>]

WEBHOOK_TOLERANCE_SECONDS = int(os.getenv("STRIPE_WEBHOOK_TOLERANCE", "300"))


def _parse_signature_header(header: str) -> tuple:
    """(timestamp:int|None, [v1 hex signatures])."""
    ts, sigs = None, []
    for part in (header or "").split(","):
        key, sep, value = part.strip().partition("=")
        if not sep:
            continue
        if key == "t":
            try:
                ts = int(value)
            except (TypeError, ValueError):
                ts = None
        elif key == "v1":
            sigs.append(value)
    return ts, sigs


def verify_webhook_signature(payload: bytes, header: str, secret: str, now=None) -> bool:
    """True only for a genuine, recent, untampered Stripe delivery.

    Fails closed on every missing piece — no secret, no header, no timestamp,
    no v1 signature — because the alternative (treating unverifiable as
    acceptable) turns this into an open "grant me a subscription" endpoint.

    The timestamp window is what stops replay: a captured request can't be
    resent tomorrow, even though its signature stays mathematically valid
    forever.
    """
    if not secret or not header or payload is None:
        return False
    ts, sigs = _parse_signature_header(header)
    if ts is None or not sigs:
        return False

    now_ts = int((now or datetime.now(timezone.utc)).timestamp())
    if abs(now_ts - ts) > WEBHOOK_TOLERANCE_SECONDS:
        return False

    signed = b"%d.%s" % (ts, payload)
    expected = hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()
    # compare_digest, not ==, so a wrong signature can't be recovered byte by
    # byte from response timing. any() over all v1 values supports Stripe's
    # secret rotation, which sends two.
    return any(hmac.compare_digest(expected, s) for s in sigs)
