from kafka import KafkaConsumer
import os, json


if __name__ == "__main__":
    print("Starting Earthquake Consumer")
    TOPIC_NAME = os.environ.get("TOPIC_NAME", "earthquake")
    KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "localhost:9092")

    print("Setting up Kafka consumer at {}".format(KAFKA_BROKER_URL))
    consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=[KAFKA_BROKER_URL])

    print("Waiting for msg...")
    for msg in consumer:
        event = json.loads(msg.value.decode("utf-8"))
        # add print for checking
        print("M{} {} at {}".format(event["magnitude"], event["place"], event["event_time"]))
