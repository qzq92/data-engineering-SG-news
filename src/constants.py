import os

# API params
PATH_LAST_PROCESSED = "./data/last_processed.json"
KAFKA_PRODUCER_CLUSTER_PORT = 9092
KAFKA_PRODUCER_EXT_PORT = 9094
URL = f"https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml&category=6936"
URL_TOPIC = "CNA_Business"


# POSTGRES PARAMS. Defaults to localhost if POSTGRES_DOCKER_USER exist
user_name = os.getenv("POSTGRES_DOCKER_USER", "localhost")
POSTGRES_URL = f"jdbc:postgresql://{user_name}:5432/postgres"
POSTGRES_PASSWORD =  "admin"
POSTGRES_USER = "admin"
POSTGRES_DB = "admin"
POSTGRES_TABLE_NAME = URL_TOPIC

SPARK_PKG = "org.postgresql:postgresql:42.7.3,org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.4"

# Columns to expand
COLUMNS_TO_EXPAND= []

# Columns to normalise
COLUMNS_TO_NORMALIZE = []

# Important fields to extract
COLUMNS_TO_KEEP = [
    "title",
    "id",
    "link",
    "published",
    "description",
]

# FIELDS OF DATABASE
DB_FIELDS = COLUMNS_TO_KEEP + COLUMNS_TO_NORMALIZE + COLUMNS_TO_EXPAND
