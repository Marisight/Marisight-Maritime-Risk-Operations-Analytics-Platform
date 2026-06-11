CREATE DATABASE IF NOT EXISTS marisight_db;

CREATE TABLE IF NOT EXISTS marisight_db.kafka_seismic_queue
(
    source_id      Nullable(String),
    source_catalog Nullable(String),
    lastupdate     Nullable(String),
    time           Nullable(String),
    flynn_region   Nullable(String),
    lat            Nullable(Float64),
    lon            Nullable(Float64),
    depth          Nullable(Float64),
    evtype         Nullable(String),
    auth           Nullable(String),
    mag            Nullable(Float64),
    magtype        Nullable(String),
    unid           Nullable(String),
    action         Nullable(String),
    __deleted      Nullable(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'marisight.public.seismic_events',
    kafka_group_name = 'clickhouse-consumer-v3',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_skip_broken_messages = 1;

CREATE TABLE IF NOT EXISTS marisight_db.seismic_events
(
    source_id      Nullable(String),
    source_catalog Nullable(String),
    lastupdate     Nullable(String),
    event_time     Nullable(String),
    flynn_region   Nullable(String),
    lat            Nullable(Float64),
    lon            Nullable(Float64),
    depth          Nullable(Float64),
    evtype         Nullable(String),
    auth           Nullable(String),
    mag            Nullable(Float64),
    magtype        Nullable(String),
    unid           Nullable(String),
    action         Nullable(String)
)
ENGINE = MergeTree()
ORDER BY tuple();

CREATE MATERIALIZED VIEW IF NOT EXISTS marisight_db.mv_seismic_consumer
TO marisight_db.seismic_events
AS SELECT
    source_id,
    source_catalog,
    lastupdate,
    time AS event_time,
    flynn_region,
    lat,
    lon,
    depth,
    evtype,
    auth,
    mag,
    magtype,
    unid,
    action
FROM marisight_db.kafka_seismic_queue
WHERE __deleted IS NULL OR __deleted != 'true';