# Databricks notebook source
# MAGIC %md
# MAGIC # Gold dimensions (Lakeflow Declarative Pipeline)
# MAGIC SCD Type 2 history for users, artists and tracks, plus a plain copy of DimDate.
# MAGIC Run through the `spotify_azure_etl` pipeline, not as a normal notebook.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql.functions import col

CATALOG = spark.conf.get("source_catalog", "spotify_catalog")

# table -> (business key, column that orders changes)
SCD2_DIMENSIONS = {
    "DimUser": ("user_id", "updated_at"),
    "DimArtist": ("artist_id", "updated_at"),
    "DimTrack": ("track_id", "updated_at"),
}


def build_scd2_dimension(name: str, key: str, sequence_col: str):
    """Declare a streaming target and an auto-CDC flow from the silver table."""
    source_view = f"{name}_silver_source"

    @dp.view(name=source_view)
    def _source():
        return spark.readStream.table(f"{CATALOG}.silver.{name}")

    dp.create_streaming_table(name=name)
    dp.create_auto_cdc_flow(
        target=name,
        source=source_view,
        keys=[key],
        sequence_by=col(sequence_col),
        stored_as_scd_type=2,
    )


for _name, (_key, _seq) in SCD2_DIMENSIONS.items():
    build_scd2_dimension(_name, _key, _seq)


@dp.materialized_view(name="DimDate")
def dim_date():
    return spark.read.table(f"{CATALOG}.silver.DimDate")
