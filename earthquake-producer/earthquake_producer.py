"""Produce USGS real-time earthquake events to the 'earthquake' kafka topic.

Source: USGS Earthquake Hazards Program GeoJSON summary feeds (free, no API key).
https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php
"""

import json
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TOPIC_NAME = os.environ.get("TOPIC_NAME")
SLEEP_TIME = int(os.environ.get("SLEEP_TIME", 60))
# Feed name, e.g. all_hour, all_day, 2.5_day, 4.5_week, significant_month
FEED = os.environ.get("USGS_FEED", "all_day")
FEED_URL = f"https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{FEED}.geojson"


def ms_to_iso(ms):
    """USGS times are epoch milliseconds; convert to an ISO-8601 UTC string."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def to_event(feature):
    """Flatten one GeoJSON feature into a record matching the Cassandra table."""
    props = feature["properties"]
    longitude, latitude, depth = feature["geometry"]["coordinates"]
    return {
        "id": feature["id"],
        "place": props.get("place"),
        "magnitude": props.get("mag"),
        "mag_type": props.get("magType"),
        "event_time": ms_to_iso(props.get("time")),
        "updated_time": ms_to_iso(props.get("updated")),
        "latitude": latitude,
        "longitude": longitude,
        "depth_km": depth,
        "significance": props.get("sig"),
        "felt_reports": props.get("felt"),
        "alert_level": props.get("alert"),
        "tsunami": props.get("tsunami"),
        "status": props.get("status"),
        "event_type": props.get("type"),
        "network": props.get("net"),
        "url": props.get("url"),
    }


def get_earthquakes():
    response = requests.get(FEED_URL, timeout=30)
    response.raise_for_status()
    return [to_event(f) for f in response.json()["features"]]


def run():
    iterator = 0
    # event id -> last 'updated' time we sent, so we only resend revised events
    sent = {}
    print("Setting up Earthquake producer at {}".format(KAFKA_BROKER_URL))
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER_URL],
        # Encode all values as JSON
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    while True:
        try:
            events = get_earthquakes()
            new_or_updated = [e for e in events if sent.get(e["id"]) != e["updated_time"]]
            for event in new_or_updated:
                producer.send(TOPIC_NAME, value=event)
                sent[event["id"]] = event["updated_time"]
            producer.flush()
            # Forget events that have dropped out of the feed window
            current_ids = {e["id"] for e in events}
            sent = {k: v for k, v in sent.items() if k in current_ids}
            print(
                "Iteration {}: {} events in feed, sent {} new/updated".format(
                    iterator, len(events), len(new_or_updated)
                )
            )
        except requests.RequestException as err:
            print("Failed to fetch USGS feed: {}".format(err))
        time.sleep(SLEEP_TIME)
        iterator += 1


if __name__ == "__main__":
    run()
