#!/usr/bin/env python3
"""Download Met Museum images locally, resize to thumbnails, update met_data.js."""
import os, re, ssl, time, urllib.request, io
from PIL import Image

IMG_DIR = "images"
THUMB_SIZE = (256, 256)

os.makedirs(IMG_DIR, exist_ok=True)

with open("met_data.js", "r") as f:
    content = f.read()

# Find all Met image URLs in met_data.js
pat = re.compile(r'"img":\s*"(https://images\.metmuseum\.org/[^"]+)"')
urls = pat.findall(content)
print(f"Found {len(urls)} images to download")

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

downloaded = 0
skipped = 0
errors = 0
consecutive_err = 0

for i, url in enumerate(urls):
    fname = url.split("/")[-1]
    fpath = os.path.join(IMG_DIR, fname)

    if os.path.exists(fpath) and os.path.getsize(fpath) > 0:
        skipped += 1
        continue

    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })
        with urllib.request.urlopen(req, context=ctx, timeout=15) as resp:
            data = resp.read()

        img = Image.open(io.BytesIO(data))
        img = img.convert("RGB")
        img.thumbnail(THUMB_SIZE, Image.LANCZOS)
        img.save(fpath, "JPEG", quality=82)

        downloaded += 1
        consecutive_err = 0
        time.sleep(0.08)

    except urllib.error.HTTPError as e:
        errors += 1
        consecutive_err += 1
        if e.code == 403 and consecutive_err >= 5:
            print(f"  Rate limited — backing off 30s at {i}/{len(urls)}...")
            time.sleep(30)
            consecutive_err = 0
        else:
            time.sleep(0.2)
    except Exception:
        errors += 1
        time.sleep(0.2)

    if (i + 1) % 200 == 0:
        print(f"  {i+1}/{len(urls)} processed ({downloaded} new, {skipped} cached, {errors} errors)")

print(f"\nDownload done: {downloaded} new, {skipped} cached, {errors} errors")

# Rewrite met_data.js: replace Met URLs with local paths for files that exist
def repl(m):
    url = m.group(1)
    fname = url.split("/")[-1]
    if os.path.exists(os.path.join(IMG_DIR, fname)):
        return f'"img": "images/{fname}"'
    return m.group(0)

new_content = pat.sub(repl, content)
with open("met_data.js", "w") as f:
    f.write(new_content)

local_count = new_content.count('"img":"images/')
print(f"Updated met_data.js: {local_count} images now use local paths")
