# Databricks Task: unzip
# Entpackt alle ZIPs aus dem Download-Ordner ins Zielverzeichnis im Volume.

dbutils.widgets.text("catalog", "energy_weather", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("volume", "energy_weather", "Volume")
dbutils.widgets.text("subfolder", "solar_zip/dwd_solar_hourly", "Quellordner (ZIPs)")
dbutils.widgets.text("dest_folder", "solar_unzipped", "Zielordner (entpackt)")

CATALOG   = dbutils.widgets.get("catalog").strip()
SCHEMA    = dbutils.widgets.get("schema").strip()
VOLUME    = dbutils.widgets.get("volume").strip()
SRC_DIR   = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{dbutils.widgets.get('subfolder').strip().strip('/')}"
DEST_DIR  = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{dbutils.widgets.get('dest_folder').strip().strip('/')}"

import glob, os, zipfile

os.makedirs(DEST_DIR, exist_ok=True)
zips = sorted(glob.glob(f"{SRC_DIR}/*.zip"))
print(f"{len(zips)} ZIP-Dateien in {SRC_DIR}")

errors = []
for i, path in enumerate(zips, 1):
    try:
        with zipfile.ZipFile(path) as z:
            z.extractall(DEST_DIR)   # überschreibt vorhandene Dateien (wie unzip -o)
    except zipfile.BadZipFile as e:
        errors.append((os.path.basename(path), str(e)))
    if i % 50 == 0 or i == len(zips):
        print(f"[{i}/{len(zips)}] entpackt ...")

if errors:
    raise RuntimeError(f"{len(errors)} korrupte ZIPs: {errors}")
print(f"Alle Dateien entpackt nach {DEST_DIR}")