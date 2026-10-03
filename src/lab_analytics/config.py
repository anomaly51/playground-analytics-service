from __future__ import annotations

import os
from dataclasses import dataclass

_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def _boolean(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    raise ValueError(f"{name} must be a boolean value")


def _positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    value = default if raw is None else int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    value = default if raw is None else float(raw)
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _first_env(*names: str, default: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None:
            return value
    return default


@dataclass(frozen=True, slots=True)
class Settings:
    http_host: str
    http_port: int
    kafka_enabled: bool
    kafka_bootstrap_servers: str
    kafka_input_topic: str
    kafka_analytics_topic: str
    kafka_trace_topic: str
    kafka_group_id: str
    kafka_client_id: str
    kafka_publish_timeout_seconds: float
    kafka_probe_interval_seconds: float
    kafka_probe_timeout_seconds: float
    processing_retry_max_seconds: float
    postgres_url: str = "postgresql://airflow:airflow@postgres:5432/airflow"
    postgres_operation_timeout_seconds: float = 3.0
    postgres_probe_interval_seconds: float = 2.0
    postgres_probe_timeout_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            http_host=_first_env(
                "FLASHDROP_ANALYTICS_HOST", "LAB_ANALYTICS_HOST", default="0.0.0.0"
            ),
            http_port=_positive_int("LAB_ANALYTICS_PORT", 8002),
            kafka_enabled=_boolean("LAB_KAFKA_ENABLED", True),
            kafka_bootstrap_servers=_first_env(
                "FLASHDROP_KAFKA_BOOTSTRAP_SERVERS",
                "LAB_KAFKA_BOOTSTRAP_SERVERS",
                default="kafka:29092",
            ),
            kafka_input_topic=_first_env(
                "FLASHDROP_KAFKA_ORDERS_TOPIC",
                "LAB_KAFKA_MESSAGE_TOPIC",
                default="flashdrop.orders.v1",
            ),
            kafka_analytics_topic=_first_env(
                "FLASHDROP_KAFKA_ANALYTICS_TOPIC",
                "LAB_KAFKA_ANALYTICS_TOPIC",
                default="flashdrop.analytics.v1",
            ),
            kafka_trace_topic=_first_env(
                "FLASHDROP_KAFKA_TRACE_TOPIC",
                "LAB_KAFKA_TRACE_TOPIC",
                default="flashdrop.traces.v1",
            ),
            kafka_group_id=_first_env(
                "FLASHDROP_ANALYTICS_GROUP_ID",
                "LAB_KAFKA_GROUP_ID",
                default="flashdrop-analytics-v1",
            ),
            kafka_client_id=_first_env(
                "FLASHDROP_ANALYTICS_KAFKA_CLIENT_ID",
                "LAB_ANALYTICS_KAFKA_CLIENT_ID",
                default="flashdrop-analytics",
            ),
            kafka_publish_timeout_seconds=_positive_float(
                "LAB_KAFKA_PUBLISH_TIMEOUT_SECONDS", 3.0
            ),
            kafka_probe_interval_seconds=_positive_float(
                "LAB_KAFKA_PROBE_INTERVAL_SECONDS", 2.0
            ),
            kafka_probe_timeout_seconds=_positive_float(
                "LAB_KAFKA_PROBE_TIMEOUT_SECONDS", 1.0
            ),
            processing_retry_max_seconds=_positive_float(
                "FLASHDROP_ANALYTICS_RETRY_MAX_SECONDS", 10.0
            ),
            postgres_url=_first_env(
                "FLASHDROP_ANALYTICS_POSTGRES_URL",
                "LAB_ANALYTICS_POSTGRES_URL",
                default="postgresql://airflow:airflow@postgres:5432/airflow",
            ),
            postgres_operation_timeout_seconds=_positive_float(
                "LAB_ANALYTICS_POSTGRES_OPERATION_TIMEOUT_SECONDS", 3.0
            ),
            postgres_probe_interval_seconds=_positive_float(
                "LAB_ANALYTICS_POSTGRES_PROBE_INTERVAL_SECONDS", 2.0
            ),
            postgres_probe_timeout_seconds=_positive_float(
                "LAB_ANALYTICS_POSTGRES_PROBE_TIMEOUT_SECONDS", 1.0
            ),
        )
