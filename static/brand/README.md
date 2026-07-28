# Brand assets

Source files for the Nexus Card Co branding. Drop the two originals here:

| File | What it is |
|---|---|
| `icon-1024.png` | Square app icon (the four-diamond mark on the dark rounded square) |
| `banner.png` | Wide banner with the wordmark and tagline |

Then generate the derivatives:

```bash
python tools/make_brand_assets.py
```

That writes the sized variants the templates reference — a 32px favicon, a
180px apple-touch-icon, a 512px PWA icon, and a bounded Open Graph image.

**Why derivatives rather than serving the originals:** a 1024x1024 PNG is a
poor favicon (browsers download the whole thing for a 16px tab icon), and
Discord re-fetches an `og:image` on every unfurl, so an oversized banner is
paid for on every link share.

Generated files are committed deliberately — Railway builds from the
Dockerfile and does not run this script, so the sized assets have to exist in
the repo.
