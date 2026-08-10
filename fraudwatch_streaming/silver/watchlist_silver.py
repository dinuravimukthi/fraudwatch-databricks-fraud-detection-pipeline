from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, current_timestamp, upper, to_timestamp

@dp.table (
    name = "fraudwatch.silver.watchlist",
    table_properties = {"quality": "silver", "pipelines.reset.allowed": "false"},
    comment = "Parsed and cleaned watchlist data"
)
def watchlist_silver() -> DataFrame:

    bronze_df = spark.readStream.table("fraudwatch.bronze.watchlist")

    transformed_df = bronze_df.select(
        upper(col("watchlist_id")).alias("watchlist_id"),
        col("watch_type"),
        upper(col("entity_id")).alias("entity_id"),
        upper(col("action")).alias("action"),
        col("city"),
        col("country"),
        to_timestamp(col("effective_from"), "dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
        col("reason_code"),
        col("reason_description"),
        col("reported_by"),
        col("reported_source"),
        upper(col("risk_level")).alias("risk_level"),
        col("status"),
        col("_rescued_data"),
        col("file_path").alias("source_file"),
        col("ingestion_ts").alias("bronze_ingestion_timestamp"),
        current_timestamp().alias("silver_ingestion_timestamp")
    )

    return transformed_df

