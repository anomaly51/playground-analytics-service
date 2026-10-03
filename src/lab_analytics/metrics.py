from prometheus_client import Counter, Gauge, Histogram

MESSAGES = Counter(
    "lab_analytics_messages_total",
    "Consumed FlashDrop order records by outcome.",
    ("status",),
)
PROCESSING_DURATION = Histogram(
    "lab_analytics_processing_duration_seconds",
    "Time spent projecting and publishing an order record.",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)
PUBLISHED = Counter(
    "lab_analytics_kafka_events_total",
    "Published Kafka events by topic and outcome.",
    ("topic", "status"),
)
KAFKA_READY = Gauge(
    "lab_analytics_kafka_ready",
    "Whether the Kafka consumer and producer are ready.",
)
POSTGRES_READY = Gauge(
    "lab_analytics_postgres_ready",
    "Whether PostgreSQL persistence is ready.",
)
POSTGRES_WRITES = Counter(
    "lab_analytics_postgres_writes_total",
    "PostgreSQL writes by table and outcome.",
    ("table", "status"),
)
RETRIES = Counter(
    "lab_analytics_processing_retries_total",
    "Transient record processing retries.",
)
