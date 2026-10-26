# Databricks notebook source
# MAGIC %md
# MAGIC # Silver layer
# MAGIC Reads the bronze parquet files (landed by Azure Data Factory) with Auto Loader,
# MAGIC cleans them and writes Delta tables to `spotify_catalog.silver`.

# COMMAND ----------

import os
import sys

from pyspark.sql.functions import col, regexp_replace, when

# Add the bundle root to Python's import path so `utils` can be imported.
sys.path.append(os.path.join(os.getcwd(), "..", ".."))

from utils.transformation import reusable

STORAGE_ACCOUNT = "viviazure"
BRONZE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
CATALOG = "spotify_catalog"

# COMMAND ----------

# Per-table settings. Checkpoint / path values are kept exactly as originally run so
# existing checkpoints keep working. `path=None` means a managed table.
TABLES = {
    "DimUser": {"checkpoint": "DimUser/checkpoint/data", "path": "DimUser/data", "dedupe": ["user_id"]},
    "DimArtist": {"checkpoint": "DimArtist/checkpoint/artist", "path": "DimArtist/data", "dedupe": ["artist_id"]},
    "DimTrack": {"checkpoint": "DimTrack/_checkpoints/track_2", "path": None, "dedupe": None},
    "DimDate": {"checkpoint": "DimDate/checkpoint/date2", "path": "DimDate/data", "dedupe": None},
    "FactStream": {"checkpoint": "FactStream/checkpoint/data", "path": "FactStream/data", "dedupe": None},
}


def read_bronze(name: str):
    """Auto Loader stream over the bronze parquet folder for one table."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{SILVER}/{name}/checkpoint")
        .option("schemaEvolutionMode", "addNewColumns")
        .load(f"{BRONZE}/{name}")
    )


def write_silver(df, name: str, cfg: dict):
    """Append the stream to a Delta table, processing all available data then stopping."""
    writer = (
        df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{SILVER}/{cfg['checkpoint']}")
        .trigger(once=True)
    )
    if cfg["path"]:
        writer = writer.option("path", f"{SILVER}/{cfg['path']}")
    return writer.toTable(f"{CATALOG}.silver.{name}")


# COMMAND ----------

for name, cfg in TABLES.items():
    df = reusable().dropColumns(read_bronze(name), ["_rescued_data"])

    if cfg["dedupe"]:
        df = df.dropDuplicates(cfg["dedupe"])

    if name == "DimTrack":
        # Bucket tracks by length and strip dashes from names.
        df = df.withColumn(
            "durationFlag",
            when(col("duration_sec") < 150, "Short").when(col("duration_sec") < 300, "Medium").otherwise("Long"),
        ).withColumn("track_name", regexp_replace(col("track_name"), "-", ""))

    write_silver(df, name, cfg).awaitTermination()
