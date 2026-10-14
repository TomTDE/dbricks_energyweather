# Databricks Task: bronze
# Lädt die entpackten DWD-CSVs und die Realerzeugungs-CSV 1:1 als Delta (Bronze).

from pyspark.sql import functions as F
import re

dbutils.widgets.text("catalog", "energy_weather", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("volume", "energy_weather", "Volume")
dbutils.widgets.text("dest_folder", "solar_unzipped", "Ordner mit entpackten DWD-Dateien")
dbutils.widgets.text("realerz_path", "Realisierte_Erzeugung_202501010000_202601010000_Stunde.csv", "CSV-Datei Realerzeugung (im Volume-Root)")

CATALOG   = dbutils.widgets.get("catalog").strip()
SCHEMA    = dbutils.widgets.get("schema").strip()
VOLUME    = dbutils.widgets.get("volume").strip()
UNZIP_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{dbutils.widgets.get('dest_folder').strip().strip('/')}"
REALERZ   = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/{dbutils.widgets.get('realerz_path').strip().strip('/')}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

LOAD_TS = F.current_timestamp()

import re

def sanitize_columns(df):
    """Macht Spaltennamen Delta-tauglich."""
    mapping = {
        "ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
        "Ã¤": "ae", "Ã¶": "oe", "Ã¼": "ue",   # ISO-8859-1-Mojibake
        "Ä": "Ae", "Ö": "Oe", "Ü": "Ue",
    }
    seen, taken = {}, set()
    for col in df.columns:
        name = col
        for bad, good in mapping.items():
            name = name.replace(bad, good)

        name = re.sub(r"[ ,;{}()\n\t=]+", "_", name)
        name = re.sub(r"[\[\]]", "", name)
        name = re.sub(r"_+", "_", name).strip("_")
        base, n = name, 1
        while name in taken:
            n += 1
            name = f"{base}_{n}"
        taken.add(name)
        if name != col:
            seen[col] = name
    for alt, neu in seen.items():
        df = df.withColumnRenamed(alt, neu)
    return df

def bronze(path: str, table: str, pattern: bool = False):
    src = f"{UNZIP_DIR}/{path}" if pattern else path
    df = spark.read.csv(src, header=True, sep=";", encoding="ISO-8859-1", inferSchema=False)   # alles als String: Bronze verändert nicht
    df = (df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn("_load_ts", LOAD_TS))
    df = sanitize_columns(df)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.{table}")
    print(f"bronze.{table}: {df.count()} Zeilen")

bronze("produkt_st_stunde_*.txt", "solar_stunden_bronze", pattern=True)
bronze("Metadaten_Geographie_*.txt", "solar_stationen_bronze", pattern=True)
bronze(REALERZ, "realerzeugung_bronze")