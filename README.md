# Restock Dashboard

A role-gated web dashboard for visualizing Pokemon card restock data, with Discord OAuth2 login and per-user watermarking.

---

## Setup

### 1. Discord Developer Portal
1. Go to https://discord.com/developers/applications
2. Create a new application (or use your existing bot's application)
3. Go to **OAuth2** → copy your **Client ID** and **Client Secret**
4. Under **Redirects**, add: `https://yourdomain.up.railway.app/callback`
5. For local dev also add: `http://localhost:8000/callback`

### 2. Environment variables
Copy `.env.example` to `.env` and fill in:

| Variable | Description |
|---|---|
| `DATABASE_URL` | Same Postgres URL your bot uses |
| `DISCORD_CLIENT_ID` | From Discord Developer Portal |
| `DISCORD_CLIENT_SECRET` | From Discord Developer Portal |
| `DISCORD_REDIRECT_URI` | Your Railway URL + `/callback` |
| `DISCORD_GUILD_ID` | Your server ID (`1406738815854317658`) |
| `REQUIRED_ROLE_ID` | Role ID that grants dashboard access |
| `SESSION_SECRET` | Any long random string (use `python -c "import secrets; print(secrets.token_hex(32))"`) |

### 3. Run locally
```bash
pip install -r requirements.txt
uvicorn main:app --reload
```
Visit http://localhost:8000

---

## Deploy to Railway

1. Push this folder to a **new GitHub repository** (separate from your bot)
2. In Railway → New Project → Deploy from GitHub repo
3. Select your new dashboard repo
4. Add all environment variables from `.env.example` in Railway's Variables tab
5. Railway will auto-detect the Dockerfile and deploy
6. Copy the generated Railway domain and set it as `DISCORD_REDIRECT_URI`
7. Add that same domain to your Discord app's OAuth2 redirect list

---

## Channel → Region mapping

In `main.py`, update `channel_to_region` if your channel names differ:

```python
channel_to_region = {
    "nova-restock-information": "NOVA",
    "md-restock-information":   "MD",
    "dc-restock-information":   "DC",
    "rva-restock-information":  "RVA",
}
```