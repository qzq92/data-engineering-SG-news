from src.constants import (
    URL,
    URL_TOPIC,
    PATH_LAST_PROCESSED,
    KAFKA_PRODUCER_CLUSTER_PORT,
    KAFKA_PRODUCER_EXT_PORT
)
from .transformations import transform_row
from kafka import KafkaProducer
from typing import List, Callable, Tuple
import feedparser
import kafka.errors
import json
import datetime
import logging

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO, force=True)


def get_latest_timestamp_from_file() -> datetime:
    """
    Gets the latest timestamp from the last_processed.json file
    """
    with open(PATH_LAST_PROCESSED, "r") as file:
        data = json.load(file)
        if "last_processed" in data:
            return data["last_processed"]
        else:
            return datetime.datetime.min


def update_last_processed_file(data: List[dict]) -> None:
    """
    Updates the last_processed.json file with the latest timestamp. Since the comparison is strict on the field date_de_publication, we set the new last_processed day to the latest timestamp minus one day.
    """
    publication_dates_as_timestamps = [
        datetime.datetime.strptime(row["date_de_publication"], "%Y-%m-%d")
        for row in data
    ]
    last_processed = max(publication_dates_as_timestamps) - datetime.timedelta(days=1)
    last_processed_as_string = last_processed.strftime("%Y-%m-%d")
    # Overwrite data
    with open(PATH_LAST_PROCESSED, "w") as file:
        json.dump({"last_processed": last_processed_as_string}, file)


def get_all_data(last_processed_timestamp: datetime.datetime, url: str) -> Tuple[List[dict], datetime]:
    """Get all data from configured URL and update processed timestamp info when data is available.

    Args:
        last_processed_timestamp (datetime.datetime): _description_

    Returns:
        List[dict]: _description_
    """
    full_data_list = []
    data = feedparser.parse(url)
    current_articles_list = data["articles"]
    full_data_list.extend(current_articles_list)
    
    if len(full_data_list):
        # If it is the case, change the last_processed_timestamp parameter to the date of publication minus one day. In case of duplicates, they will be filtered
        last_timestamp = current_articles_list[0]["published"]
        last_processed_timestamp = datetime.datetime.strptime(last_timestamp, "%a, %d %b %Y %H:%M:%S %z")


    logging.info(f"Got {len(full_data_list)} results from the API")

    return full_data_list, last_processed_timestamp


def deduplicate_data(data: List[dict]) -> List[dict]:
    return list({v["id"]: v for v in data}.values())


def query_data() -> List[dict]:
    """
    Queries the data from the API of interest. Function is called under __main__
    """
    last_processed_date = get_latest_timestamp_from_file()
    full_data = get_all_data(last_processed_timestamp=last_processed_date, url=URL)
    full_data = deduplicate_data(full_data)
    if full_data:
        update_last_processed_file(full_data)
    return full_data


def process_data(row) -> Callable:
    """
    Processes the data from the API
    """
    return transform_row(row)


def create_kafka_producer(cluster_port: int, ext_port: int) -> KafkaProducer:
    """Creates the Kafka producer object (broker) in the cluster using specified cluster port within a cluster and enable external connection to cluster via ext_port
    """
    cluster_port = str(cluster_port)
    ext_port = str(ext_port)
    try:
        # A broker that facilitates transactions between consumers and producers
        producer = KafkaProducer(bootstrap_servers=[f"kafka:{cluster_port}"])
    except kafka.errors.NoBrokersAvailable:
        logging.info(
            f"No brokers available, localhost is used via external port {ext_port}"
        )
        # External comms
        producer = KafkaProducer(bootstrap_servers=[f"localhost:{ext_port}"])

    return producer

# Callbacks for success delivery
def on_send_success():
    logging.info("Deliveries sent successfully")

# Callbacks for
def on_send_error(excp):
    logging.error('Deliveries encountered error', exc_info=excp)

def stream():
    """
    Writes the API data to Kafka topic 'CNA_rss_business' upon transformation
    """
    producer = create_kafka_producer(
        cluster_port=KAFKA_PRODUCER_CLUSTER_PORT,
        ext_port=KAFKA_PRODUCER_EXT_PORT
        )
    results = query_data()
    # Transform data after querying and send for consumption by looping through
    kafka_data_full = map(process_data, results)

    #publish data
    for kafka_data in kafka_data_full:
        producer.send(URL_TOPIC, json.dumps(kafka_data).encode("utf-8")).add_callback(on_send_success).add_errback(on_send_error)

    producer.flush()
    
# Main function
if __name__ == "__main__":
    stream()
