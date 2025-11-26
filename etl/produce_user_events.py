import csv
from pathlib import Path

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

TOPIC = "user_events"
CSV_PATH = Path("data/user_events.csv")
VALUE_SCHEMA_PATH = Path("schemas/user_events_value.avsc")


def load_schema(path):
    with open(path) as f:
        return f.read()


def identity(obj, ctx):
    return obj


def get_producer():
    schema_registry_conf = {"url": "http://localhost:8081"}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    value_schema_str = load_schema(VALUE_SCHEMA_PATH)
    value_avro_serializer = AvroSerializer(
        schema_registry_client,
        value_schema_str,
        identity,
    )

    key_schema_str = '"int"'
    key_avro_serializer = AvroSerializer(
        schema_registry_client,
        key_schema_str,
        identity,
    )

    return SerializingProducer(
        {
            "bootstrap.servers": "localhost:9092",
            "key.serializer": key_avro_serializer,
            "value.serializer": value_avro_serializer,
        }
    )


def main():
    producer = get_producer()

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            value = {
                "event_time": row["event_time"],
                "user_id": int(row["user_id"]),
                "session_id": row["session_id"],
                "event_type": row["event_type"],
                "channel": row["channel"],
                "premium_amount": float(row["premium_amount"]),
            }

            key = int(row["user_id"])

            producer.produce(
                topic=TOPIC,
                key=key,
                value=value,
            )

    producer.flush()
    print("Sent events using AvroSerializer")


if __name__ == "__main__":
    main()
