# Databricks Task: download
# Lädt alle ZIPs der DWD-Stundendaten Solar ins UC-Volume.

dbutils.widgets.text("catalog", "energy_weather", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("volume", "energy_weather", "Volume")
dbutils.widgets.text("subfolder", "solar_zip/dwd_solar_hourly", "Unterordner im Volume")
dbutils.widgets.text("max_workers", "8", "Parallele Downloads")
dbutils.widgets.dropdown("create_volume", "true", ["true", "false"], "Volume anlegen, falls nicht vorhanden")
dbutils.widgets.dropdown("skip_existing", "true", ["true", "false"], "Vorhandene Dateien überspringen")

CATALOG        = dbutils.widgets.get("catalog").strip()
SCHEMA         = dbutils.widgets.get("schema").strip()
VOLUME         = dbutils.widgets.get("volume").strip()
SUBFOLDER      = dbutils.widgets.get("subfolder").strip().strip("/")
MAX_WORKERS    = int(dbutils.widgets.get("max_workers"))
CREATE_VOLUME  = dbutils.widgets.get("create_volume") == "true"
SKIP_EXISTING  = dbutils.widgets.get("skip_existing") == "true"

BASE_URL   = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/solar/"
TARGET_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{SUBFOLDER}".rstrip("/")
RETRIES    = 3

print(f"Quelle: {BASE_URL}")
print(f"Ziel:   {TARGET_DIR}")

if CREATE_VOLUME:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

import os
os.makedirs(TARGET_DIR, exist_ok=True)

# --- Indexseite parsen -------------------------------------------------
import requests
from html.parser import HTMLParser
from urllib.parse import urljoin

class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.links.append(href)

session = requests.Session()
session.headers.update({"User-Agent": "databricks-dwd-downloader/1.0"})

resp = session.get(BASE_URL, timeout=60)
resp.raise_for_status()

parser = LinkParser()
parser.feed(resp.text)
zip_urls = sorted({urljoin(BASE_URL, h) for h in parser.links if h.lower().endswith(".zip")})
print(f"{len(zip_urls)} ZIP-Dateien gefunden")

# --- Download ----------------------------------------------------------
import shutil, tempfile, time
from concurrent.futures import ThreadPoolExecutor, as_completed

def download_one(url: str):
    """Lädt eine Datei in Temp-Speicher und kopiert sie ins Volume.
    Rückgabe: (dateiname, status, bytes, fehlertext)"""
    name   = url.rsplit("/", 1)[-1]
    target = f"{TARGET_DIR}/{name}"
    last_err = None

    for attempt in range(1, RETRIES + 1):
        tmp_path = None
        try:
            with session.get(url, stream=True, timeout=(15, 120)) as r:
                r.raise_for_status()
                remote_size = int(r.headers.get("Content-Length", -1))

                if SKIP_EXISTING and remote_size >= 0 and os.path.exists(target) \
                        and os.path.getsize(target) == remote_size:
                    return (name, "skipped", remote_size, None)

                with tempfile.NamedTemporaryFile(delete=False, suffix=".part") as tmp:
                    tmp_path = tmp.name
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        tmp.write(chunk)

            local_size = os.path.getsize(tmp_path)
            if remote_size >= 0 and local_size != remote_size:
                raise IOError(f"Größe stimmt nicht: erwartet {remote_size}, erhalten {local_size}")

            shutil.copyfile(tmp_path, target)
            return (name, "downloaded", local_size, None)
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            time.sleep(2 * attempt)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    return (name, "failed", 0, last_err)

results, start = [], time.time()
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futures = [pool.submit(download_one, u) for u in zip_urls]
    for i, f in enumerate(as_completed(futures), 1):
        res = f.result()
        results.append(res)
        if res[1] == "failed":
            print(f"[{i}/{len(zip_urls)}] FEHLER {res[0]}: {res[3]}")

downloaded = [r for r in results if r[1] == "downloaded"]
skipped    = [r for r in results if r[1] == "skipped"]
failed     = [r for r in results if r[1] == "failed"]

print(f"Fertig in {time.time() - start:.1f}s | geladen: {len(downloaded)} "
      f"({sum(r[2] for r in downloaded)/1e6:.1f} MB) | übersprungen: {len(skipped)} | Fehler: {len(failed)}")

if failed:
    raise RuntimeError(f"{len(failed)} Downloads fehlgeschlagen: {[r[0] for r in failed]}")