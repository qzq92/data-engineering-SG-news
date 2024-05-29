# Spark is use for processing the incoming data offered by Kafka streaming.

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)
from pyspark.sql.functions import from_json, col
from src.constants import POSTGRES_URL, POSTGRES_TABLE_NAME, POSTGRES_PROPERTIES, DB_FIELDS, URL_TOPIC, KAFKA_PRODUCER_CLUSTER_PORT
import logging


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s:%(funcName)s:%(levelname)s:%(message)s"
)


def create_spark_session() -> SparkSession:
    """Instantiate a spark session if there is none, otherwise get from existing sessions with specified configuration for spark jars packages, a list of Maven coordinates of jars to include on the driver and executor classpaths of Spark.
    
    Returns:
        SparkSession: _description_
    """
    spark = (
        SparkSession.builder.appName("PostgreSQL Connection with PySpark")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4",
        )
        .getOrCreate()
    )

    logging.info("Spark session created successfully")
    return spark


def create_initial_dataframe(spark_session):
    """
    Reads the streaming data via spark session readStream attributes and creates the initial dataframe accordingly.
    """
    try:
        # Loads the streaming data by subscribing
        df = (
            spark_session.readStream.format("kafka")
            .option("kafka.bootstrap.servers", f"kafka:{KAFKA_PRODUCER_CLUSTER_PORT}")
            .option("subscribe", str(URL_TOPIC)) #Only 1 topic, expand value for multiple topics
            .option("startingOffsets", "earliest")
            .load()
        )
        logging.info("Initial dataframe created successfully from kafka stream.")
    except Exception as e:
        logging.warning(f"Initial dataframe couldn't be created due to exception: {e}")
        raise e

    return df


def create_final_dataframe(df):
    """
    Modifies the initial dataframe, and creates the final dataframe.
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


def start_streaming(df_parsed, spark):
    """
    Starts the streaming to table spark_streaming.URL_ in postgres
    """
    # Read existing data from PostgreSQL
    existing_data_df = spark.read.jdbc(
        POSTGRES_URL, POSTGRES_TABLE_NAME, properties=POSTGRES_PROPERTIES
    )

    unique_column = "reference_fiche"

    logging.info("Start streaming ...")
    query = df_parsed.writeStream.foreachBatch(
        lambda batch_df, _: (
            batch_df.join(
                existing_data_df, batch_df[unique_column] == existing_data_df[unique_column], "leftanti"
            )
            .write.jdbc(
                POSTGRES_URL, POSTGRES_TABLE_NAME, "append", properties=POSTGRES_PROPERTIES
            )
        )
    ).trigger(once=True) \
        .start()

    return query.awaitTermination()


def write_to_postgres():
    spark = create_spark_session()
    df = create_initial_dataframe(spark)
    df_final = create_final_dataframe(df)
    start_streaming(df_final, spark=spark)


if __name__ == "__main__":
    write_to_postgres()
