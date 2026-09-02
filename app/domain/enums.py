"""Stable domain enumerations shared by API, storage, and worker layers."""

from enum import StrEnum


class NotificationStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    DEAD = "dead"


class ErrorClass(StrEnum):
    SUCCESS = "success"
    RETRYABLE = "retryable"
    NON_RETRYABLE = "non_retryable"
