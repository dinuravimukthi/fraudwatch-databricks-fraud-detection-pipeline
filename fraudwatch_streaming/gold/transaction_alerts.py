from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, concat_ws, lit
from pyspark.sql.dataframe import DataFrame


@dp.table(
    name="fraudwatch.gold.transaction_alerts",
    table_properties={
        "quality": "gold"
        #"pipelines.reset.allowed": "false"
    },
    comment="Alert details for transactions exceeding the client-specififed threshold"
)
def transaction_alerts() -> DataFrame:
    transactions_df = spark.readStream.table("fraudwatch.silver.transactions")
    customers_df = spark.read.table("fraudwatch.silver.customers")

    joined_df = transactions_df.join(
        customers_df,
        transactions_df.customer_id == customers_df.customer_id,
        "left"
    ).filter(
        col("amount") > col("transaction_limit")
    ).select(
        concat_ws("-", lit("ALERT"), col("transaction_id")).alias("alert_id"),
        lit("LIMIT_EXCEEDED_TRANSACTION").alias("alert_type"),
        current_timestamp().alias("alert_timestamp"),

        transactions_df.transaction_id,
        transactions_df.customer_id,
        customers_df.email.alias("customer_email"),
        concat_ws(" ", col("first_name"), col("last_name")).alias("customer_name"),
        transactions_df.amount.alias("transaction_amount"),
        customers_df.transaction_limit,
        transactions_df.currency,
        transactions_df.merchant_name,
        transactions_df.merchant_category,
        transactions_df.transaction_type,
        transactions_df.payment_channel,
        transactions_df.city,
        transactions_df.country,
        transactions_df.is_international,
        transactions_df.transaction_timestamp,
        transactions_df.status
    )

    return joined_df