
CREATE TABLE azki.user_events
(
    event_time     DateTime,
    user_id        UInt32,
    session_id     String,
    event_type     LowCardinality(String),
    channel        LowCardinality(String),
    premium_amount UInt64,
    order_id        UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (user_id, event_time);

CREATE TABLE azki.third_order
(
    id             UInt64,
    user_id        UInt32,
    car_model      String,
    status         LowCardinality(String),
    created_at     DateTime,
    updated_at     DateTime,
    created_date   Date
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_date)
ORDER BY (created_date, id);


CREATE TABLE azki.body_order
(
    id             UInt64,
    user_id        UInt32,
    car_model      String,
    status         LowCardinality(String),
    created_at     DateTime,
    updated_at     DateTime,
    created_date   Date
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_date)
ORDER BY (created_date, id);

CREATE TABLE azki.medical_order
(
    id             UInt64,
    user_id        UInt32,
    insured_name   String,
    status         LowCardinality(String),
    created_at     DateTime,
    updated_at     DateTime,
    created_date   Date 
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_date)
ORDER BY (created_date, id);

CREATE TABLE azki.fire_order
(
    id             UInt64,
    user_id        UInt32,
    property_city  String,
    status         LowCardinality(String),
    created_at     DateTime,
    updated_at     DateTime,
    created_date   Date 
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_date)
ORDER BY (created_date, id);


CREATE TABLE azki.financial_order
(
    id              UInt64,
    user_id         UInt32,
    premium_amount  UInt64,
    discount_amount UInt64,
    final_price     UInt64,
    status          LowCardinality(String),
    created_at      DateTime,
    updated_at      DateTime,
    created_date    Date
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_date)
ORDER BY (created_date, id);


CREATE TABLE azki.orders_union
(
    order_id      UInt64,
    user_id       UInt32,
    product_type  LowCardinality(String),
    car_model     Nullable(String),
    insured_name  Nullable(String),
    property_city Nullable(String),
    status        LowCardinality(String),
    created_at    DateTime,
    updated_at    DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (order_id);


CREATE MATERIALIZED VIEW azki.mv_third_orders_union
TO azki.orders_union AS
SELECT
    id                         AS order_id,
    user_id,
    'third'                    AS product_type,
    car_model                  AS car_model,
    CAST(NULL AS Nullable(String)) AS insured_name,
    CAST(NULL AS Nullable(String)) AS property_city,

    status,
    created_at,
    updated_at
FROM azki.third_order;


CREATE MATERIALIZED VIEW azki.mv_body_orders_union
TO azki.orders_union AS
SELECT
    id                         AS order_id,
    user_id,
    'body'                     AS product_type,

    car_model                  AS car_model,
    CAST(NULL AS Nullable(String)) AS insured_name,
    CAST(NULL AS Nullable(String)) AS property_city,

    status,
    created_at,
    updated_at
FROM azki.body_order;


CREATE MATERIALIZED VIEW azki.mv_medical_orders_union
TO azki.orders_union AS
SELECT
    id                         AS order_id,
    user_id,
    'medical'                  AS product_type,

    CAST(NULL AS Nullable(String)) AS car_model,
    insured_name               AS insured_name,
    CAST(NULL AS Nullable(String)) AS property_city,

    status,
    created_at,
    updated_at
FROM azki.medical_order;


CREATE MATERIALIZED VIEW azki.mv_fire_orders_union
TO azki.orders_union AS
SELECT
    id                         AS order_id,
    user_id,
    'fire'                     AS product_type,

    CAST(NULL AS Nullable(String)) AS car_model,
    CAST(NULL AS Nullable(String)) AS insured_name,
    property_city              AS property_city,

    status,
    created_at,
    updated_at
FROM azki.fire_order;



CREATE TABLE azki.user_purchase_events
(
    event_time      DateTime,
    user_id         UInt32,
    session_id      String,
    channel         LowCardinality(String),
    event_type      LowCardinality(String),
    order_id        UInt64,
    product_type    LowCardinality(String),
    car_model       Nullable(String),
    insured_name    Nullable(String),
    property_city   Nullable(String),
    premium_amount      UInt64,
    discount_amount     UInt64,
    final_price         UInt64,
    status    LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, order_id, user_id);


CREATE MATERIALIZED VIEW azki.mv_user_purchase_events
TO azki.user_purchase_events AS
SELECT
    e.event_time,
    e.user_id,
    e.session_id,
    e.channel,
    e.event_type,
    e.order_id,
    o.product_type,
    o.car_model,
    o.insured_name,
    o.property_city,
    fo.premium_amount,
    fo.discount_amount,
    fo.final_price,
    fo.status 
FROM azki.user_events AS e
LEFT JOIN azki.orders_union     AS o
    ON e.order_id = o.order_id
LEFT JOIN azki.financial_order  AS fo
    ON e.order_id = fo.id
WHERE e.event_type = 'purchase';
