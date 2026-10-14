# Databricks Task: silver
# Bereinigt die Bronze-Tabellen: Trim, deutsche Zahlenformate, Timestamps,
# Stationszeitraum-Join. Ergebnis: typsichere Silver-Tabellen.

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "energy_weather", "Catalog")
dbutils.widgets.text("schema", "default", "Schema")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA  = dbutils.widgets.get("schema").strip()

B = lambda t: spark.table(f"{CATALOG}.{SCHEMA}.{t}")

def de_number(colname: str):
    """Spalte im deutschen Zahlenformat (1.234,56) in double umwandeln."""
    return F.regexp_replace(F.regexp_replace(F.trim(F.col(colname)), r"\.", ""), ",", ".").cast("double")

# --- 1) Stationsmetadaten: Bronze -> Silver ------------------------------
df_solar_meta_geo = (B("solar_stationen_bronze")
    .withColumn("von_datum_d", F.try_to_date("von_datum", "yyyyMMdd"))
    .withColumn("bis_datum_d", F.try_to_date("bis_datum", "yyyyMMdd"))
    .withColumnRenamed("Stations_id", "StationsID")
    .withColumnRenamed("Geogr.Breite", "GeogrBreite")
    .withColumnRenamed("Geogr.Laenge", "GeogrLaenge")
    .withColumn("StationsID", F.trim("StationsID"))
    .drop("_source_file", "_load_ts"))

# --- 2) Stundenprodukt Solar: Bronze -> Silver ---------------------------
df_solar_prod = (B("solar_stunden_bronze")
    .withColumn("MESS_DATUM_DATE", F.to_date(F.substring("MESS_DATUM", 1, 8), "yyyyMMdd"))
    .withColumn("MESS_DATUM_DATETIME", F.to_timestamp("MESS_DATUM", "yyyyMMddHH:mm"))
    .withColumn("STATIONS_ID", F.trim("STATIONS_ID"))
    .withColumn("stunde", F.date_trunc("hour", "MESS_DATUM_DATETIME"))   # für Equi-Join in Gold
    .withColumn("jahr", F.year("MESS_DATUM_DATE")))

for c in ["ATMO_LBERG", "FD_LBERG", "FG_LBERG", "SD_LBERG"]:
    df_solar_prod = df_solar_prod.withColumn(c, de_number(c))

# --- 3) Realerzeugung: Bronze -> Silver -----------------------------------
renames = {
    "Datum_von": "DatumVon", "Datum_bis": "DatumBis",
    "Biomasse_MWh_Berechnete_Aufloesungen": "Biomasse_MWh",
    "Wasserkraft_MWh_Berechnete_Aufloesungen": "Wasserkraft_MWh",
    "Wind_Offshore_MWh_Berechnete_Aufloesungen": "WindOffshore_MWh",
    "Wind_Onshore_MWh_Berechnete_Aufloesungen": "WindOnshore_MWh",
    "Photovoltaik_MWh_Berechnete_Aufloesungen": "Photovoltaik_MWh",
    "Sonstige_Erneuerbare_MWh_Berechnete_Aufloesungen": "SonstigeErneuerbare_MWh",
    "Kernenergie_MWh_Berechnete_Aufloesungen": "Kernenergie_MWh",
    "Braunkohle_MWh_Berechnete_Aufloesungen": "Braunkohle_MWh",
    "Steinkohle_MWh_Berechnete_Aufloesungen": "Steinkohle_MWh",
    "Erdgas_MWh_Berechnete_Aufloesungen": "Erdgas_MWh",
    "Pumpspeicher_MWh_Berechnete_Aufloesungen": "Pumpspeicher_MWh",
    "Sonstige_Konventionelle_MWh_Berechnete_Aufloesungen": "SonstigeKonventionelle_MWh",
}

df_realerz = B("realerzeugung_bronze")
for alt, neu in renames.items():
    df_realerz = df_realerz.withColumnRenamed(alt, neu)

df_realerz = (df_realerz
    .withColumn("DatumVon", F.to_timestamp("DatumVon", "dd.MM.yyyy HH:mm"))
    .withColumn("DatumBis", F.to_timestamp("DatumBis", "dd.MM.yyyy HH:mm"))
    .withColumn("stunde", F.date_trunc("hour", "DatumVon")))   # für Equi-Join in Gold

for c in renames.values():
    if c not in ("DatumVon", "DatumBis", "Kernenergie_MWh"):
        df_realerz = df_realerz.withColumn(c, de_number(c))

# --- 4) Silver schreiben --------------------------------------------------
(df_solar_meta_geo.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.solar_stationen_silver"))
(df_solar_prod.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.solar_stunden_silver"))
(df_realerz.write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.realerzeugung_silver"))

print("Silver-Tabellen aktualisiert")