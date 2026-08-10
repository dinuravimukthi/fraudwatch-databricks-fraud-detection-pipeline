from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="fraudwatch.gold.transaction_count_by_minute_sliding_window",
    table_properties={
        "quality": "gold"
        #"pipelines.reset.allowed": "false"
    },
    comment="Transaction count aggregation by minute using sliding window method"
)
def transaction_count_by_minute() -> DataFrame:
    transactions_df = spark.readStream.table("fraudwatch.silver.transactions")
    
    transactions_with_watermark = transactions_df.withWatermark("transaction_timestamp", "5 minutes")

    transaction_count_df = transactions_with_watermark.groupBy(
        F.window("transaction_timestamp", "5 minute", "1 minute") # window size=5, sliding interval=1, Every 1 min do the aggregation of last 5 min
    ).agg(
        F.count("*").alias("transaction_count")
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("transaction_count")
    )

    return transaction_count_df