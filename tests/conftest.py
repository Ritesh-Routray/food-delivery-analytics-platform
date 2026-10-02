"""
conftest.py — Shared pytest fixtures across all test modules.
"""
from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def master_data():
    """Session-scoped master data fixture (expensive to generate)."""
    from simulator.master_data import MasterData
    return MasterData.get()


@pytest.fixture
def sample_event_placed():
    """A minimal valid order_placed event dict."""
    return {
        "event_id": "test-event-001",
        "event_type": "order_placed",
        "schema_version": 1,
        "event_ts": "2026-10-02T12:00:00.000Z",
        "produced_ts": "2026-10-02T12:00:00.100Z",
        "order_id": "ORD-10000001",
        "city": "Bengaluru",
        "payload": {
            "customer_id": "CUS-000001",
            "restaurant_id": "RST-000001",
            "order_value": "350.0",
            "item_count": "2",
            "payment_method": "upi",
        },
    }


@pytest.fixture
def sample_event_delivered():
    """A minimal valid order_delivered event dict."""
    return {
        "event_id": "test-event-002",
        "event_type": "order_delivered",
        "schema_version": 1,
        "event_ts": "2026-10-02T12:45:00.000Z",
        "produced_ts": "2026-10-02T12:45:00.200Z",
        "order_id": "ORD-10000001",
        "city": "Bengaluru",
        "payload": {
            "rider_id": "RID-000001",
            "customer_id": "CUS-000001",
            "delivery_ts": "2026-10-02T12:45:00.000Z",
            "distance_km": "5.2",
            "tip_amount": "20.0",
        },
    }


@pytest.fixture
def bad_event_null_city():
    """An event with null city — should be caught by Silver validation."""
    return {
        "event_id": "test-bad-001",
        "event_type": "order_placed",
        "schema_version": 1,
        "event_ts": "2026-10-02T12:00:00.000Z",
        "produced_ts": "2026-10-02T12:00:00.100Z",
        "order_id": "ORD-10000002",
        "city": None,
        "payload": {},
    }


@pytest.fixture
def bad_event_invalid_type():
    """An event with an invalid event_type — should go to DLQ."""
    return {
        "event_id": "test-bad-002",
        "event_type": "order_teleported",  # not a valid type
        "schema_version": 1,
        "event_ts": "2026-10-02T12:00:00.000Z",
        "produced_ts": "2026-10-02T12:00:00.100Z",
        "order_id": "ORD-10000003",
        "city": "Mumbai",
        "payload": {},
    }
