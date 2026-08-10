from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, current_timestamp
import json

@dp.table (
    name = "fraudwatch.bronze.transactions",
    table_properties = {"quality": "bronze", "pipelines.reset.allowed": "false"},
    comment = "Transactions raw data stream"
)
def transactions_bronze() -> DataFrame:
    # Get kafka connection details
    kafka_config_json = dbutils.secrets.get(scope="fraudwatch-scope", key="kafka-connection-details")
    kafka_config = json.loads(kafka_config_json)

    bootstrap_servers = kafka_config['bootstrap_servers']
    topic = kafka_config['topic']
    api_key = kafka_config['api_key']
    api_secret = kafka_config['api_secret']

    jaas_config=f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{api_key}' password='{api_secret}';"

    # Create spark stream to load data from kafka
    kafka_stream = (
        spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', bootstrap_servers)
        .option('kafka.security.protocol', 'SASL_SSL')
        .option('kafka.sasl.mechanism', 'PLAIN')
        .option('kafka.sasl.jaas.config', jaas_config)
        .option('subscribe', topic)
        .option('startingOffsets', 'earliest')
        .load()
    )

    parsed_streaming_df = kafka_stream.select(
        col('key').cast('string'),
        col('value').cast('string'),
        col('topic'),
        col('partition'),
        col('offset'),
        col('timestamp'),
        col('timestampType'),
        current_timestamp().alias('ingestion_ts')
    )

    return parsed_streaming_df

