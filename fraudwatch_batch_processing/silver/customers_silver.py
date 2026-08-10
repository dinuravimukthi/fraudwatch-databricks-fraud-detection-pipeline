from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, to_date
import pyspark.sql.types as T

__customers_rules_drop = {
    "valid_customer_id": "customer_id IS NOT NULL"
}

@dp.table(
    name="fraudwatch.silver.customers",
    table_properties={"quality": "silver", "pipeline.reset.allowed": "false"},
    comment="Parsed and cleaned customers data"
)
@dp.expect_all_or_drop(__customers_rules_drop)
def customers_silver():
    bronze_df = spark.readStream.table('fraudwatch.bronze.customers')

    transformed_df = bronze_df.select(
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("gender"),
        col("age"),
        col("city"),
        col("state"),
        col("country"),
        col("annual_income"),
        col("customer_segment"),
        to_date(col("account_open_date"), "yyyy-MM-dd").alias("account_open_date"),
        col("risk_score"),
        col("preferred_spending_min"),
        col("preferred_spending_max"),
        col("preferred_city"),
        col("preferred_country"),
        col("trusted_device_id"),
        col("card_number"),
        col("card_type"),
        col("email"),
        col("transaction_limit"),
        col("update_timestamp"),
        current_timestamp().alias("silver_ingestion_timestamp")
    )
    
    return transformed_df