# Databricks Task: unzip
# Entpackt alle ZIPs aus dem Download-Ordner ins Zielverzeichnis im Volume
# plus den MaStR-Gesamtdatenexport nach {volume}/mastr_unzipped/.

dbutils.widgets.text("catalog", "energy_weather", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("volume", "energy_weather", "Volume")
dbutils.widgets.text("subfolder", "solar_zip/dwd_solar_hourly", "Quellordner (ZIPs)")
dbutils.widgets.text("dest_folder", "solar_unzipped", "Zielordner (entpackt)")
dbutils.widgets.text("mastr_folder", "mastr", "Quellordner MaStR-ZIP")
dbutils.widgets.text("mastr_zip", "Gesamtdatenexport_20261013_26.2.zip", "Dateiname MaStR-Gesamtdatenexport")
dbutils.widgets.text("mastr_dest_folder", "mastr_unzipped", "Zielordner MaStR (entpackt)")

CATALOG   = dbutils.widgets.get("catalog").strip()
SCHEMA    = dbutils.widgets.get("schema").strip()
VOLUME    = dbutils.widgets.get("volume").strip()
VOL_ROOT  = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
SRC_DIR   = f"{VOL_ROOT}/{dbutils.widgets.get('subfolder').strip().strip('/')}"
DEST_DIR  = f"{VOL_ROOT}/{dbutils.widgets.get('dest_folder').strip().strip('/')}"
mastr_src = f"{VOL_ROOT}/{dbutils.widgets.get('mastr_folder').strip().strip('/')}/{dbutils.widgets.get('mastr_zip').strip()}"
mastr_dst = f"{VOL_ROOT}/{dbutils.widgets.get('mastr_dest_folder').strip().strip('/')}"

import glob, os, zipfile

def unzip_all(zip_paths, dest, label):
    os.makedirs(dest, exist_ok=True)
    errors = []
    for i, path in enumerate(zip_paths, 1):
        try:
            with zipfile.ZipFile(path) as z:
                z.extractall(dest)   # überschreibt vorhandene Dateien (wie unzip -o)
        except zipfile.BadZipFile as e:
            errors.append((os.path.basename(path), str(e)))
        if i % 50 == 0 or i == len(zip_paths):
            print(f"[{label}] [{i}/{len(zip_paths)}] entpackt ...")
    return errors

# --- 1) DWD-Solar-ZIPs -----------------------------------------------------
zips = sorted(glob.glob(f"{SRC_DIR}/*.zip"))
print(f"{len(zips)} ZIP-Dateien in {SRC_DIR}")
errors = unzip_all(zips, DEST_DIR, "dwd")

# --- 2) MaStR-Gesamtdatenexport (eine große ZIP) ----------------------------
print(f"Entpacke MaStR: {mastr_src} -> {mastr_dst}")
if not os.path.exists(mastr_src):
    raise FileNotFoundError(f"MaStR-ZIP nicht gefunden: {mastr_src}")
errors_mastr = unzip_all([mastr_src], mastr_dst, "mastr")

if errors or errors_mastr:
    raise RuntimeError(f"Korrupte ZIPs: DWD={errors}, MaStR={errors_mastr}")
print(f"Alle Dateien entpackt nach {DEST_DIR} bzw. {mastr_dst}")