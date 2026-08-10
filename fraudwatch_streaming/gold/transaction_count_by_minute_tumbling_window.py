from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="fraudwatch.gold.transaction_count_by_minute_tumbling_window",
    table_properties={
        "quality": "gold"
        #"pipelines.reset.allowed": "false"
    },
    comment="Transaction count aggregation by minute using tumbling window method"
)
def transaction_count_by_minute() -> DataFrame:
    transactions_df = spark.readStream.table("fraudwatch.silver.transactions")
    
    transactions_with_watermark = transactions_df.withWatermark("transaction_timestamp", "5 minutes")

    transaction_count_df = transactions_with_watermark.groupBy(
        F.window("transaction_timestamp", "1 minute")
    ).agg(
        F.count("*").alias("transaction_count")
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("transaction_count")
    )

    return transaction_count_df