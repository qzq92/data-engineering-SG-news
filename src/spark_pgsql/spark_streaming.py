# Spark is use for processing the incoming data offered by Kafka streaming.
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)
from pyspark.sql.functions import from_json, col
from src.constants import POSTGRES_URL, POSTGRES_TABLE_NAME, DB_FIELDS, URL_TOPIC, KAFKA_PRODUCER_CLUSTER_PORT, SPARK_PKG, POSTGRES_USER, POSTGRES_PASSWORD

import logging

# Log config
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)

def create_spark_session() -> SparkSession:
    """Instantiate a spark session with specified configuration for spark jars packages, a list of Maven coordinates of jars to include on the driver and executor classpaths of Spark. Refer to documentation on the supported maven coordinates.
    
    Returns:
        SparkSession: Instance of SparkSession.
    """
    spark = (
        SparkSession.builder.appName("PostgreSQL Connection with PySpark")
        .config(
            "spark.jars.packages",
            f"{SPARK_PKG}",
        )
        .getOrCreate()
    )

    logging.info("Spark session created successfully")
    return spark


def create_initial_dataframe(spark_session: SparkSession)-> DataFrame:
    """
    Reads the incoming streaming data from Kafka as a subscriber via readStream attributes and creates the initial dataframe accordingly.
    """
    try:
        # Loads the streaming data by subscribing to 1 topic in Kafka with starting offset as earliest
        df = (
            spark_session.readStream.format("kafka")\
            .option("kafka.bootstrap.servers", f"kafka:{KAFKA_PRODUCER_CLUSTER_PORT}")\
            .option("subscribe", str(URL_TOPIC))\
            .option("startingOffsets", "earliest")\
            .option("failOnDataLoss", "true")\
            .load()
        )
        logging.info("Initial dataframe created successfully via subscribing to kafka input stream.")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")
        raise e

    return df

def create_final_dataframe(df: DataFrame) -> DataFrame:
    """Modifies the initial dataframe loaded with stream data by the function above, and creates the final dataframe with specified schema which would be streamed out to Database
    """
    # Define schema for dataframe
    schema = StructType(
        [StructField(field_name, StringType(), True) for field_name in DB_FIELDS]
    )
    df_out = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    return df_out


def start_streaming(df_parsed: DataFrame, spark):
    """
    Starts the streaming to PostgresDB
    """
    # Read existing data from PostgreSQL
    existing_data_df = spark.read.jdbc(
        POSTGRES_URL, POSTGRES_TABLE_NAME, properties = {
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD
        }
    )

    unique_column = DB_FIELDS[0]

    logging.info("Start spark dataframe streaming to Postgres DB")

    # Apply batch functions to the microbatch output data of the streaming query to be aoended to database. Trigger is set to process all the available data and then stop on its own. Use left antijoin as a workaround to directly append new streams.
    query = df_parsed.writeStream.foreachBatch(
        lambda batch_df, _: (
            batch_df.join(
                existing_data_df, on = batch_df[unique_column] == existing_data_df[unique_column], how="leftantijoin"
            )
            .write.jdbc(
                POSTGRES_URL, POSTGRES_TABLE_NAME, "append", properties={
                    "user": POSTGRES_USER,
                    "password": POSTGRES_PASSWORD
                }
            )
        )
    ).trigger(availableNow=True) \
        .start()

    return query.awaitTermination()


def write_to_postgres():
    """Main function which instantiates spark sessions and creates a dataframe with streamed data from Kafka. Subsequently streams the data to Postgres DB.
    """
    spark = create_spark_session()
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df)
    start_streaming(df_final, spark=spark)


if __name__ == "__main__":
    write_to_postgres()
