"""Generate the sized brand assets the templates reference.

Run after dropping the two source files into static/brand/:

    python tools/make_brand_assets.py

Sizing matters more than it looks. A browser fetches the favicon for a 16px
tab slot, and Discord re-fetches the Open Graph image every time someone drops
the dashboard link in chat — serving 1024px originals for either means paying
full freight for a thumbnail, repeatedly.

Outputs are committed to the repo on purpose: Railway builds from the
Dockerfile and never runs this script, so the files have to already exist.
"""

import os
import sys

from PIL import Image

BRAND_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "static", "brand")

ICON_SRC = os.path.join(BRAND_DIR, "icon-1024.png")
BANNER_SRC = os.path.join(BRAND_DIR, "banner.png")

# (output name, edge length). 32 covers the browser tab; 180 is the
# apple-touch-icon size iOS uses for a home-screen shortcut; 512 is the PWA /
# high-DPI size.
ICON_SIZES = [("favicon-32.png", 32), ("icon-180.png", 180), ("icon-512.png", 512)]

# Discord renders a large embed around 1200px wide; anything beyond that is
# downloaded and thrown away on every unfurl.
OG_MAX_WIDTH = 1200


def _load(path: str):
    if not os.path.isfile(path):
        print(f"  MISSING: {os.path.relpath(path, BRAND_DIR)} — see static/brand/README.md")
        return None
    img = Image.open(path)
    img.load()
    print(f"  read {os.path.basename(path)}: {img.width}x{img.height} {img.mode}")
    return img


def main() -> int:
    print(f"Brand assets in {BRAND_DIR}")
    made, missing = [], False

    icon = _load(ICON_SRC)
    if icon is None:
        missing = True
    else:
        icon = icon.convert("RGBA")
        if icon.width != icon.height:
            print(f"  WARNING: icon is {icon.width}x{icon.height}, not square — "
                  "it will be squashed, not cropped")
        for name, size in ICON_SIZES:
            out = os.path.join(BRAND_DIR, name)
            icon.resize((size, size), Image.LANCZOS).save(out, "PNG", optimize=True)
            made.append((name, os.path.getsize(out)))

    banner = _load(BANNER_SRC)
    if banner is None:
        missing = True
    else:
        # Flattened onto the brand navy: Open Graph images are composited on
        # whatever background the client uses, and a transparent PNG can render
        # with white edges in a dark Discord embed.
        banner = banner.convert("RGBA")
        # Caught for real once: the source arrived stored rotated 90 degrees,
        # which resizes and saves perfectly happily and only shows up as a
        # sideways image inside a Discord embed. A wide banner is never
        # taller than it is wide, so refuse rather than publish it.
        if banner.height >= banner.width:
            print(f"  ERROR: banner is {banner.width}x{banner.height} — portrait or "
                  "square. A wide banner should be landscape; this one looks "
                  "rotated. Fix the source and re-run.")
            return 1
        if banner.width > OG_MAX_WIDTH:
            h = round(banner.height * OG_MAX_WIDTH / banner.width)
            banner = banner.resize((OG_MAX_WIDTH, h), Image.LANCZOS)
        flat = Image.new("RGB", banner.size, (17, 16, 33))
        flat.paste(banner, (0, 0), banner)
        out = os.path.join(BRAND_DIR, "og-banner.png")
        flat.save(out, "PNG", optimize=True)
        made.append(("og-banner.png", os.path.getsize(out)))
        print(f"  og-banner: {flat.width}x{flat.height} "
              f"(ratio {flat.width / flat.height:.2f}:1)")

    if made:
        print("\nWrote:")
        for name, size in made:
            print(f"  {name:20s} {size / 1024:7.1f} KB")
    if missing:
        print("\nSome sources were missing — nothing was generated for those.")
        return 1
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
