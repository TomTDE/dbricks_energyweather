# Databricks Task: gold
# Join Strahlung (silver) mit realisierter PV-Erzeugung (silver)
# und schreibt die Gold-Tabelle.

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "energy_weather", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")
dbutils.widgets.text("year", "2025", "Betrachtungsjahr")
dbutils.widgets.text("target_table", "solar_pv_compare", "Gold-Zieltabelle (Delta)")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA  = dbutils.widgets.get("schema").strip()
JAHR    = int(dbutils.widgets.get("year"))
ZIEL    = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('target_table').strip()}"

S = lambda t: spark.table(f"{CATALOG}.{SCHEMA}.{t}")

# --- 0) Join vorbereiten: Metadaten raus, Kollisionen umbenennen ----------
META_COLS = {"source_file", "load_ts"}   # technische Spalten gehören nicht ins Gold

def drop_meta(df):
    return df.drop(*[c for c in df.columns if c in META_COLS])

# --- 1) Strahlung: Stationszeitraum auflösen, Jahr filtern ----------------
df_links = drop_meta(S("solar_stunden_silver")).alias("links")

df_solar = (df_links
    .join(drop_meta(S("solar_stationen_silver")).alias("rechts"),
          on=((F.col("links.STATIONS_ID") == F.col("rechts.StationsID")) &
              (F.col("links.MESS_DATUM_DATE") >= F.col("rechts.von_datum_d")) &
              (F.col("links.MESS_DATUM_DATE") <= F.col("rechts.bis_datum_d"))),
          how="inner")
    .filter(F.col("jahr") == JAHR))

# --- 2) Equi-Join mit Realerzeugung auf Stundenebene ----------------------
df_rechts = drop_meta(S("realerzeugung_silver"))

# Namenskollisionen (außer Join-Key) auf der rechten Seite mit '_re' suffixen
dup = [c for c in df_rechts.columns if c in set(df_solar.columns)] # and c != "stunde"
for c in dup:
    df_rechts = df_rechts.withColumnRenamed(c, f"{c}_re")

df_gold = (df_solar.alias("links")
    .join(df_rechts.alias("rechts"),
          #on=F.col("links.stunde") == F.col("rechts.stunde"),
          on=F.col("stunde") == F.col("stunde_re"),
          how="inner"))

# --- 3) Gold schreiben ----------------------------------------------------
(df_gold.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(ZIEL))

print(f"Geschrieben: {ZIEL} ({df_gold.count()} Zeilen), "
      f"umbenannte Kollisionen: {dup or 'keine'}")