"""
test_simulator.py — Unit tests for the event simulator.

Tests:
  - Order lifecycle event generation
  - Chaos injection (all 5 types)
  - Master data seeding
  - Load multiplier logic
"""
from __future__ import annotations

import pytest

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def master():
    from simulator.master_data import MasterData
    return MasterData.get()


@pytest.fixture
def chaos_config_full():
    from simulator.chaos import ChaosConfig
    return ChaosConfig(
        duplicate_rate=1.0,   # force all injections for testing
        late_event_rate=1.0,
        out_of_order_rate=1.0,
        schema_drift_rate=1.0,
        bad_value_rate=1.0,
        chaos_mode=True,
    )


@pytest.fixture
def chaos_config_off():
    from simulator.chaos import ChaosConfig
    return ChaosConfig(chaos_mode=False)


@pytest.fixture
def sample_event():
    """A minimal valid event dict."""
    return {
        "event_id": "test-uuid-1234",
        "event_type": "order_placed",
        "schema_version": 1,
        "event_ts": "2026-10-02T12:00:00.000Z",
        "produced_ts": "2026-10-02T12:00:00.100Z",
        "order_id": "ORD-99999999",
        "city": "Bengaluru",
        "payload": {
            "customer_id": "CUS-000001",
            "restaurant_id": "RST-000001",
            "order_value": 350.0,
            "item_count": 2,
            "payment_method": "upi",
        },
    }


# ── Master data tests ─────────────────────────────────────────────────────────

class TestMasterData:
    def test_restaurant_count(self, master):
        assert len(master.restaurants) == 500

    def test_rider_count(self, master):
        assert len(master.riders) == 300

    def test_customer_count(self, master):
        assert len(master.customers) == 5000

    def test_all_cities_indexed(self, master):
        cities = ["Bengaluru", "Mumbai", "Delhi", "Hyderabad"]
        for city in cities:
            assert city in master.restaurants_by_city
            assert len(master.restaurants_by_city[city]) > 0

    def test_random_restaurant_returns_city_match(self, master):
        r = master.random_restaurant("Bengaluru")
        assert r.city == "Bengaluru"

    def test_singleton(self, master):
        from simulator.master_data import MasterData
        assert master is MasterData.get()


# ── Chaos injection tests ─────────────────────────────────────────────────────

class TestChaosInjector:
    def test_no_chaos_when_disabled(self, chaos_config_off, sample_event):
        from simulator.chaos import ChaosInjector
        injector = ChaosInjector(chaos_config_off)
        results = injector.inject(sample_event)
        assert len(results) == 1
        assert results[0] == sample_event

    def test_duplicate_produces_two_events(self, chaos_config_full, sample_event):
        from simulator.chaos import ChaosInjector
        injector = ChaosInjector(chaos_config_full)
        results = injector.inject(sample_event)
        # With rate=1.0 and all injections, should have >= 2 events
        assert len(results) >= 2
        # Both should have the same event_id (duplicate)
        event_ids = {e["event_id"] for e in results}
        assert len(event_ids) == 1  # same event_id

    def test_late_event_backdates_event_ts(self, sample_event):
        from simulator.chaos import ChaosConfig, ChaosInjector
        config = ChaosConfig(late_event_rate=1.0, chaos_mode=True)
        injector = ChaosInjector(config)
        results = injector.inject(sample_event)
        # event_ts should be earlier than original
        from datetime import datetime
        original = datetime.fromisoformat(sample_event["event_ts"].replace("Z", "+00:00"))
        late = datetime.fromisoformat(results[0]["event_ts"].replace("Z", "+00:00"))
        assert late < original

    def test_bad_value_corrupts_field(self, sample_event):
        from simulator.chaos import ChaosConfig, ChaosInjector
        config = ChaosConfig(bad_value_rate=1.0, chaos_mode=True)
        injector = ChaosInjector(config)
        # Run many times to hit all bad value types
        found_corruption = False
        for _ in range(50):
            results = injector.inject(dict(sample_event, payload=dict(sample_event["payload"])))
            r = results[0]
            if r.get("city") is None or r["payload"].get("order_value", 1) <= 0:
                found_corruption = True
                break
        assert found_corruption

    def test_schema_drift_adds_field_or_bumps_version(self, sample_event):
        from simulator.chaos import ChaosConfig, ChaosInjector
        config = ChaosConfig(schema_drift_rate=1.0, chaos_mode=True)
        injector = ChaosInjector(config)
        results = injector.inject(sample_event)
        r = results[0]
        # Either schema_version was bumped or extra field added
        assert r["schema_version"] != 1 or "_extra_field" in r["payload"]

    def test_chaos_marker_added(self, sample_event):
        from simulator.chaos import ChaosConfig, ChaosInjector
        config = ChaosConfig(bad_value_rate=1.0, chaos_mode=True)
        injector = ChaosInjector(config)
        results = injector.inject(sample_event)
        assert "_chaos" in results[0]
        assert "bad_value" in results[0]["_chaos"]


# ── Event generator tests ─────────────────────────────────────────────────────

class TestOrderSimulator:
    def test_generates_at_least_two_events(self, master):
        from simulator.generator import OrderSimulator
        sim = OrderSimulator(master)
        events = sim.generate_order_events()
        assert len(events) >= 2  # at minimum: placed + (rejected OR confirmed)

    def test_first_event_is_order_placed(self, master):
        from simulator.generator import OrderSimulator
        sim = OrderSimulator(master)
        for _ in range(20):
            events = sim.generate_order_events()
            assert events[0]["event_type"] == "order_placed"

    def test_all_events_share_order_id(self, master):
        from simulator.generator import OrderSimulator
        sim = OrderSimulator(master)
        events = sim.generate_order_events()
        order_ids = {e["order_id"] for e in events}
        assert len(order_ids) == 1

    def test_event_ids_are_unique(self, master):
        from simulator.generator import OrderSimulator
        sim = OrderSimulator(master)
        events = sim.generate_order_events()
        event_ids = [e["event_id"] for e in events]
        assert len(event_ids) == len(set(event_ids))

    def test_all_events_have_required_fields(self, master):
        from simulator.generator import OrderSimulator
        required = {"event_id", "event_type", "schema_version", "event_ts",
                    "produced_ts", "order_id", "city", "payload"}
        sim = OrderSimulator(master)
        events = sim.generate_order_events()
        for event in events:
            missing = required - set(event.keys())
            assert not missing, f"Event missing fields: {missing}"

    def test_happy_path_ends_with_delivered(self, master):
        """At least some orders should complete delivery."""
        from simulator.generator import OrderSimulator
        sim = OrderSimulator(master)
        terminal_types = set()
        for _ in range(100):
            events = sim.generate_order_events()
            terminal_types.add(events[-1]["event_type"])
        assert "order_delivered" in terminal_types


# ── Pydantic schema validation tests ─────────────────────────────────────────

class TestSchemas:
    def test_valid_event_parses(self):
        from simulator.schemas import EventType, OrderEvent
        event = OrderEvent(
            event_type=EventType.ORDER_PLACED,
            event_ts="2026-10-02T12:00:00.000Z",
            produced_ts="2026-10-02T12:00:00.100Z",
            order_id="ORD-12345678",
            city="Mumbai",
            payload={"order_value": "350.0"},
        )
        assert event.order_id == "ORD-12345678"
        assert event.event_type == EventType.ORDER_PLACED

    def test_invalid_timestamp_raises(self):
        from pydantic import ValidationError
        from simulator.schemas import EventType, OrderEvent
        with pytest.raises(ValidationError):
            OrderEvent(
                event_type=EventType.ORDER_PLACED,
                event_ts="not-a-timestamp",
                produced_ts="2026-10-02T12:00:00.000Z",
                order_id="ORD-12345678",
                city="Mumbai",
                payload={},
            )

    def test_kafka_message_returns_bytes(self):
        from simulator.schemas import EventType, OrderEvent
        event = OrderEvent(
            event_type=EventType.ORDER_PLACED,
            event_ts="2026-10-02T12:00:00.000Z",
            produced_ts="2026-10-02T12:00:00.100Z",
            order_id="ORD-12345678",
            city="Chennai",
            payload={},
        )
        key, value = event.to_kafka_message()
        assert isinstance(key, bytes)
        assert isinstance(value, bytes)
        assert b"order_placed" in value
