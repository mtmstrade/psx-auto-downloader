import requests
import hashlib
import json
import os
from datetime import datetime

# DATASETS
FILES = {
    "index_fluctuation": "https://dps.psx.com.pk/download/text/kse_index.lis.Z",
    "all_share_index_mkt_cap": "https://dps.psx.com.pk/download/text/allshr_new.lis.Z",
    "psx_header": "https://dps.psx.com.pk/download/text/header.zip",
    "psx_header_tradable_indices": "https://dps.psx.com.pk/download/text/header2.zip",
    "companies_info": "https://dps.psx.com.pk/download/text/listed_cmp.lst.Z",
    "kse_100_index_companies": "https://dps.psx.com.pk/download/text/kse100.lis.Z",
}

# PATH (auto adjust: local vs GitHub)
if os.name == "nt":
    BASE_DIR = r"D:\MTMS ENGINE\data\psx_auto_downloader"
else:
    BASE_DIR = "data"

# 🔹 Base folder create (if not exists)
os.makedirs(BASE_DIR, exist_ok=True)

MANIFEST_FILE = os.path.join(BASE_DIR, "manifest.json")


def get_hash(content):
    return hashlib.md5(content).hexdigest()


# 🔹 Load manifest
if os.path.exists(MANIFEST_FILE):
    with open(MANIFEST_FILE, "r") as f:
        manifest = json.load(f)
else:
    manifest = {}


for name, url in FILES.items():
    print(f"\nChecking: {name}")

    # 🔹 Dataset folder create (if not exists)
    dataset_dir = os.path.join(BASE_DIR, name)
    os.makedirs(dataset_dir, exist_ok=True)

    prev = manifest.get(name, {})

    # STEP 1: HEAD
    try:
        head = requests.head(url, timeout=15, allow_redirects=True)
        last_modified = head.headers.get("Last-Modified", "")
        etag = head.headers.get("ETag", "")
        content_length = head.headers.get("Content-Length", "")
    except:
        last_modified = etag = content_length = ""

    # STEP 2: metadata check
    if (
        prev.get("etag") == etag
        and prev.get("last_modified") == last_modified
        and prev.get("content_length") == content_length
        and etag != ""
    ):
        print("skip (metadata same)")
        continue

    # STEP 3: download
    try:
        r = requests.get(url, timeout=60, allow_redirects=True)
        r.raise_for_status()
        content = r.content
    except Exception as e:
        print(f"download failed: {e}")
        continue

    # STEP 4: hash
    file_hash = get_hash(content)

    if prev.get("hash") == file_hash:
        print("skip (hash same)")
        continue

    # extension
    if url.endswith(".zip"):
        ext = ".zip"
    elif url.endswith(".lis.Z"):
        ext = ".lis.Z"
    elif url.endswith(".lst.Z"):
        ext = ".lst.Z"
    else:
        ext = ""

    # STEP 5: save
    filename = os.path.join(
        dataset_dir,
        f"{name}_{datetime.now().date()}_{file_hash[:6]}{ext}"
    )

    with open(filename, "wb") as f:
        f.write(content)

    print(f"saved: {filename}")

    # update manifest
    manifest[name] = {
        "etag": etag,
        "last_modified": last_modified,
        "content_length": content_length,
        "hash": file_hash,
        "last_saved": filename,
        "saved_at": datetime.now().isoformat()
    }


# 🔹 Save manifest
with open(MANIFEST_FILE, "w") as f:
    json.dump(manifest, f, indent=2)

print("\nDone.")