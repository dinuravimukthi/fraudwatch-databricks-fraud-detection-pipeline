from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="fraudwatch.gold.fraud_card_alerts",
    table_properties={
        "quality": "gold"
        #"pipelines.reset.allowed": "false"
    },
    comment="Alert details for card transactions in fraud-watchlist"
)
def fraud_card_alerts() -> DataFrame:
    transactions_df = spark.readStream.table("fraudwatch.silver.transactions")
    watchlist_df = spark.readStream.table("fraudwatch.silver.watchlist")
    customers_df = spark.read.table("fraudwatch.silver.customers")

    transactions_with_watermark = transactions_df.withWatermark("transaction_timestamp", "5 minutes")
    watchlist_with_watermark = watchlist_df.withWatermark("effective_from", "5 minutes")

    fraud_detected = (
        transactions_with_watermark.alias("t").join(
            watchlist_with_watermark.alias("w"),
            F.col("t.card_number") == F.col("w.entity_id"),
            "inner"
        ).join(
            customers_df,
            F.col("t.customer_id") == customers_df.customer_id,
            "left"
        ).select(
            # Alert identification
            F.concat_ws("-", F.lit("FRAUD"), F.col("transaction_id"), F.col("watchlist_id")).alias("alert_id"),
            F.lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
            F.current_timestamp().alias("alert_timestamp"),

            # Transaction details
            F.col("t.transaction_id"),
            F.col("t.customer_id"),
            customers_df.email.alias("customer_email"),
            F.concat_ws(" ", customers_df.first_name, customers_df.last_name).alias("customer_name"),
            F.col("t.card_number"),
            F.col("t.amount"),
            F.col("t.currency"),
            F.col("t.merchant_id"),
            F.col("t.merchant_name"),
            F.col("t.merchant_category"),
            F.col("t.transaction_type"),
            F.col("t.payment_channel"),
            F.col("t.device_id"),
            F.col("t.city").alias("transaction_city"),
            F.col("t.country").alias("transaction_country"),
            F.col("t.transaction_timestamp"),
            F.col("t.is_international"),
            F.col("t.status").alias("transaction_status"),

            # Watchlist details
            F.col("w.watchlist_id"),
            F.col("w.watch_type"),
            F.col("w.risk_level"),
            F.col("w.action"),
            F.col("w.reason_code"),
            F.col("w.reason_description"),
            F.col("w.effective_from").alias("watchlist_effective_from"),
            F.col("w.reported_by"),
            F.col("w.reported_source"),
            F.col("w.city").alias("watchlist_city"),
            F.col("w.country").alias("watchlist_country")
        )
    )

    return fraud_detected
