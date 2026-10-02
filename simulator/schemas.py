"""
schemas.py — Pydantic event models for the food delivery simulator.

All events share a common envelope. The payload varies by event_type.
schema_version=1 is the baseline; version=2 adds optional extra fields
(used to simulate schema drift).
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


# ── Enumerations ─────────────────────────────────────────────────────────────

class EventType(str, Enum):
    ORDER_PLACED = "order_placed"
    ORDER_CONFIRMED = "order_confirmed"
    ORDER_PREPARING = "order_preparing"
    ORDER_PICKED_UP = "order_picked_up"
    ORDER_DELIVERED = "order_delivered"
    ORDER_CANCELLED = "order_cancelled"
    ORDER_REJECTED = "order_rejected"


class City(str, Enum):
    BENGALURU = "Bengaluru"
    MUMBAI = "Mumbai"
    DELHI = "Delhi"
    HYDERABAD = "Hyderabad"
    CHENNAI = "Chennai"
    PUNE = "Pune"
    KOLKATA = "Kolkata"
    AHMEDABAD = "Ahmedabad"


class CancellationReason(str, Enum):
    CUSTOMER_CANCELLED = "customer_cancelled"
    RESTAURANT_BUSY = "restaurant_busy"
    NO_RIDER_AVAILABLE = "no_rider_available"
    PAYMENT_FAILED = "payment_failed"


# ── Payload Models ────────────────────────────────────────────────────────────

class OrderPlacedPayload(BaseModel):
    customer_id: str
    restaurant_id: str
    order_value: float
    item_count: int
    payment_method: str
    # v2 optional fields (schema drift simulation)
    promo_code: str | None = None
    is_scheduled: bool | None = None


class OrderConfirmedPayload(BaseModel):
    restaurant_id: str
    estimated_prep_minutes: int


class OrderPreparingPayload(BaseModel):
    restaurant_id: str
    actual_prep_start_ts: str


class OrderPickedUpPayload(BaseModel):
    rider_id: str
    restaurant_id: str
    pickup_ts: str


class OrderDeliveredPayload(BaseModel):
    rider_id: str
    customer_id: str
    delivery_ts: str
    distance_km: float
    tip_amount: float | None = None  # v2 field


class OrderCancelledPayload(BaseModel):
    reason: CancellationReason
    cancelled_by: str  # "customer" | "restaurant" | "system"
    refund_amount: float


class OrderRejectedPayload(BaseModel):
    restaurant_id: str
    rejection_reason: str


# ── Event Envelope ────────────────────────────────────────────────────────────

class OrderEvent(BaseModel):
    """
    Canonical event envelope. This is what the Kafka producer serialises
    to JSON and what the Spark consumer expects to parse.
    """
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: EventType
    schema_version: int = 1
    event_ts: str  # ISO-8601 UTC — when it happened (event time)
    produced_ts: str  # ISO-8601 UTC — when the producer sent it
    order_id: str
    city: str
    payload: dict[str, Any]

    @field_validator("event_ts", "produced_ts")
    @classmethod
    def must_be_iso(cls, v: str) -> str:
        # validate parseable; will raise ValueError on bad input
        datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v

    def to_kafka_message(self) -> tuple[bytes, bytes]:
        """Returns (key, value) bytes for the Kafka producer."""
        return (
            self.order_id.encode("utf-8"),
            self.model_dump_json().encode("utf-8"),
        )


# ── Restaurant Master Event ────────────────────────────────────────────────────

class RestaurantMasterEvent(BaseModel):
    """Published to restaurants.master topic for SCD2 dimension simulation."""
    restaurant_id: str
    name: str
    city: str
    cuisine: str
    commission_tier: str  # "standard" | "premium" | "enterprise"
    is_active: bool = True
    updated_ts: str
    schema_version: int = 1
