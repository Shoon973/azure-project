# Databricks notebook source
# MAGIC %md
# MAGIC # Gold FactStream
# MAGIC Builds the join SQL with Jinja, then merges it into `gold.FactStream`.
# MAGIC
# MAGIC **Check the config below against your real column names before running.**

# COMMAND ----------

import os
import sys

sys.path.append(os.path.join(os.getcwd(), "..", ".."))

from utils.query_builder import build_fact_query

CATALOG = "spotify_catalog"
FACT_KEY = "stream_id"  # unique id of a stream event

# fact_key = column on FactStream, dim_key = column on the gold dimension.
# `columns` are the dimension attributes copied onto the fact (prefixed with the alias).
DIMS = [
    {
        "table": f"{CATALOG}.gold.DimUser",
        "alias": "u",
        "fact_key": "user_id",
        "dim_key": "user_id",
        "columns": ["user_name", "country", "subscription_type"],
    },
    {
        "table": f"{CATALOG}.gold.DimTrack",
        "alias": "t",
        "fact_key": "track_id",
        "dim_key": "track_id",
        "columns": ["track_name", "artist_id", "durationFlag"],
    },
]

# COMMAND ----------

query = build_fact_query(f"{CATALOG}.silver.FactStream", DIMS)
print(query)

spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.gold.FactStream AS {query} LIMIT 0")
spark.sql(query).createOrReplaceTempView("fact_stream_enriched")

spark.sql(f"""
    MERGE INTO {CATALOG}.gold.FactStream AS tgt
    USING fact_stream_enriched AS src
    ON tgt.{FACT_KEY} = src.{FACT_KEY}
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
