from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
import json

__transaction_rules_warn = {
    "valid_transaction_amount": "amount > 0"
}

__transaction_rules_drop = {
    "valid_transaction_id": "transaction_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_card_number": "card_number IS NOT NULL",
    "valid_merchant_id": "merchant_id IS NOT NULL",
    "valid_transaction_timestamp": "transaction_timestamp IS NOT NULL"
}

@dp.table(
    name="fraudwatch.silver.transactions",
    table_properties={"quality": "silver" 
                      #"pipelines.reset.allowed": "false"
                    },
    comment="Parsed and cleaned transactions data"
)
@dp.expect_all(__transaction_rules_warn)
@dp.expect_all_or_drop(__transaction_rules_drop)
def transactions_silver() -> DataFrame:
    # Get bronze dataframe
    bronze_df = spark.readStream.table("fraudwatch.bronze.transactions")

    # Create a schema to ingest the json data from bronze
    schema = F.StructType([
        T.StructField("transaction_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("card_number", T.StringType()),
        T.StructField("merchant_id", T.StringType()),
        T.StructField("merchant_name", T.StringType()),
        T.StructField("merchant_category", T.StringType()),
        T.StructField("amount", T.DoubleType()),
        T.StructField("currency", T.StringType()),
        T.StructField("transaction_type", T.StringType()),
        T.StructField("payment_channel", T.StringType()),
        T.StructField("device_id", T.StringType()),
        T.StructField("city", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("transaction_timestamp", T.TimestampType()),
        T.StructField("is_international", T.BooleanType()),
        T.StructField("status", T.StringType())
    ])

    # Parse the bronze dataframe
    transformed_df = bronze_df.select(
        F.from_json(F.col('value'), schema).alias('data'),
        F.col('topic').alias('kafka_topic'),
        F.col('partition').alias('kafka_partition'),
        F.col('offset').alias('kafka_offset'),
        F.col('timestamp').alias('kafka_timestamp'),
        F.col('ingestion_ts').alias('bronze_ingestion_timestamp')
    ).select(
        F.col('data.*'),
        F.col('kafka_topic'),
        F.col('kafka_partition'),
        F.col('kafka_offset'),
        F.col('kafka_timestamp'),
        F.col('bronze_ingestion_timestamp')
    ).withColumn(
        'silver_ingestion_timestamp', F.current_timestamp()
    )

    return transformed_df