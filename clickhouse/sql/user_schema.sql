CREATE TABLE azki.users
(
    user_id      UInt32,
    signup_date  Date32,
    city         LowCardinality(String),
    device_type  LowCardinality(String)
)
ENGINE = ReplacingMergeTree
ORDER BY user_id;



CREATE TABLE azki.users_kafka
(
    user_id      UInt32,
    signup_date  Date32,
    city         String,
    device_type  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'mysql_cdc.azki_db.users',
    kafka_group_name = 'clickhouse_mysql_cdc_azki_db_users_consumer_v1',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 10;


CREATE MATERIALIZED VIEW azki.users_mv
TO azki.users
AS
SELECT
    user_id,
    signup_date,
    city,
    device_type
FROM azki.users_kafka;
