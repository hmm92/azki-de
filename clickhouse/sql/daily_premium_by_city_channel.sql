CREATE TABLE IF NOT EXISTS azki.daily_premium_by_city_channel
(
    `event_date` Date,
    `city` LowCardinality(String),
    `channel` LowCardinality(String),
    `total_premium` UInt64,
    `events_count` UInt64
)
ENGINE = SummingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, city, channel);


CREATE MATERIALIZED VIEW IF NOT EXISTS azki.mv_daily_premium_by_city_channel
TO azki.daily_premium_by_city_channel AS
SELECT
    toDate(e.event_time) AS event_date,
    u.city,
    e.channel,
    sum(e.premium_amount) AS total_premium,
    count() AS events_count
FROM azki.user_events AS e
LEFT JOIN azki.users AS u
    USING (user_id)
GROUP BY
    event_date,
    u.city,
    e.channel;


   
   
CREATE TABLE azki.daily_premium_by_city_channel
(
    event_date    Date,
    city          LowCardinality(String),
    channel       LowCardinality(String),
    total_premium UInt64,
    events_count  UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, city, channel);