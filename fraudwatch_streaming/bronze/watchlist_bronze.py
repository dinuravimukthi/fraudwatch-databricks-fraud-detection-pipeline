from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, current_timestamp

@dp.table (
    name = "fraudwatch.bronze.watchlist",
    table_properties = {"quality": "bronze", "pipelines.reset.allowed": "false"},
    comment = "Watchlist raw data stream using autoloader"
)
def watchlist_bronze() -> DataFrame:
    __source_path = "/Volumes/fraudwatch/source/watchlist/source_data/"
    __checkpoint_path = "/Volumes/fraudwatch/source/watchlist/checkpoints/"

    input_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(__source_path)
    )

    parsed_streaming_df = input_stream.select(
        col("watchlist_id"),
        col("watch_type"),
        col("entity_id"),
        col("action"),
        col("city"),
        col("country"),
        col("effective_from"),
        col("reason_code"),
        col("reason_description"),
        col("reported_by"),
        col("reported_source"),
        col("risk_level"),
        col("status"),
        col("_rescued_data"),
        col("_metadata.file_path").alias("file_path"),
        current_timestamp().alias("ingestion_ts")
    )

    return parsed_streaming_df

