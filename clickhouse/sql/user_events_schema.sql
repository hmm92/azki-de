
CREATE TABLE azki.user_events
(
    event_time     DateTime,
    user_id        UInt32,
    session_id     String,
    event_type     LowCardinality(String),
    channel        LowCardinality(String),
    premium_amount UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (user_id, event_time);


CREATE TABLE azki.user_events_kafka
(
    event_time     String,
    user_id        UInt32,
    session_id     String,
    event_type     String,
    channel        String,
    premium_amount UInt64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'user_events',
    kafka_group_name = 'clickhouse_user_events_consumer_v1',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;


CREATE MATERIALIZED VIEW azki.user_events_mv
TO azki.user_events
AS
SELECT
    parseDateTimeBestEffort(event_time) AS event_time,
    user_id,
    session_id,
    event_type,
    channel,
    premium_amount
FROM azki.user_events_kafka;