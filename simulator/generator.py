"""
generator.py — Main event producer loop for the food delivery simulator.

Simulates the complete order lifecycle:
  order_placed → order_confirmed → order_preparing →
  order_picked_up → order_delivered
                 ↘ order_cancelled (any stage before pickup)
                 → order_rejected  (restaurant rejects at confirmation)

Run directly:
    python generator.py

Or via Docker Compose (reads config from env vars + config.yaml).
"""
from __future__ import annotations

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from threading import Thread
from typing import Any

import structlog
import yaml
from kafka import KafkaProducer
from kafka.errors import KafkaError

from chaos import ChaosConfig, ChaosInjector, get_load_multiplier
from master_data import MasterData
from schemas import (
    City,
    EventType,
    OrderCancelledPayload,
    OrderConfirmedPayload,
    OrderDeliveredPayload,
    OrderPickedUpPayload,
    OrderPlacedPayload,
    OrderPreparingPayload,
    OrderRejectedPayload,
    RestaurantMasterEvent,
)

# ── Logging ───────────────────────────────────────────────────────────────────

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ]
)
log = structlog.get_logger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────

def load_config() -> dict[str, Any]:
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path) as f:
        cfg: dict[str, Any] = yaml.safe_load(f)

    # Environment variables override config.yaml
    env_overrides = {
        "kafka.bootstrap": os.environ.get("KAFKA_BOOTSTRAP"),
        "simulator.events_per_second": os.environ.get("EVENTS_PER_SECOND"),
        "chaos.duplicate_rate": os.environ.get("DUPLICATE_RATE"),
        "chaos.late_event_rate": os.environ.get("LATE_EVENT_RATE"),
        "chaos.out_of_order_rate": os.environ.get("OUT_OF_ORDER_RATE"),
        "chaos.bad_value_rate": os.environ.get("BAD_RECORD_RATE"),
        "chaos.schema_drift_rate": os.environ.get("SCHEMA_DRIFT_RATE"),
        "chaos.chaos_mode": os.environ.get("CHAOS_MODE"),
    }

    for key_path, value in env_overrides.items():
        if value is None:
            continue
        parts = key_path.split(".")
        d = cfg
        for part in parts[:-1]:
            d = d.setdefault(part, {})
        last = parts[-1]
        # type coercion
        existing = d.get(last)
        if isinstance(existing, bool) or value in ("true", "false"):
            d[last] = value.lower() == "true" if isinstance(value, str) else bool(value)
        elif isinstance(existing, int):
            d[last] = int(value)
        elif isinstance(existing, float):
            d[last] = float(value)
        else:
            d[last] = value

    return cfg


# ── Helpers ───────────────────────────────────────────────────────────────────

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _make_order_id() -> str:
    return f"ORD-{random.randint(10_000_000, 99_999_999)}"


def _build_event(
    event_type: EventType,
    order_id: str,
    city: str,
    payload: dict[str, Any],
    schema_version: int = 1,
) -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type.value,
        "schema_version": schema_version,
        "event_ts": _utcnow_iso(),
        "produced_ts": _utcnow_iso(),
        "order_id": order_id,
        "city": city,
        "payload": payload,
    }


# ── Order lifecycle ───────────────────────────────────────────────────────────

class OrderSimulator:
    """
    Generates the full lifecycle event sequence for a single order.
    Returns a list of event dicts in chronological order.
    """

    def __init__(self, master: MasterData) -> None:
        self.master = master

    def generate_order_events(self) -> list[dict[str, Any]]:
        city = self.master.random_city()
        restaurant = self.master.random_restaurant(city)
        customer = self.master.random_customer(city)
        order_id = _make_order_id()
        order_value = round(random.uniform(80, 1500), 2)
        payment = self.master.random_payment_method()
        events: list[dict[str, Any]] = []

        # ── 1. order_placed ──────────────────────────────────────────────────
        placed_payload = OrderPlacedPayload(
            customer_id=customer.customer_id,
            restaurant_id=restaurant.restaurant_id,
            order_value=order_value,
            item_count=random.randint(1, 8),
            payment_method=payment,
        ).model_dump(exclude_none=True)
        events.append(_build_event(EventType.ORDER_PLACED, order_id, city, placed_payload))

        time.sleep(random.uniform(0.001, 0.01))  # realistic inter-event gap

        # ── 2. order_rejected (restaurant too busy) ──────────────────────────
        if random.random() < restaurant.rejection_rate:
            rejected_payload = OrderRejectedPayload(
                restaurant_id=restaurant.restaurant_id,
                rejection_reason=random.choice([
                    "restaurant_closed", "item_unavailable", "too_many_orders"
                ]),
            ).model_dump()
            events.append(_build_event(EventType.ORDER_REJECTED, order_id, city, rejected_payload))
            return events

        # ── 3. order_confirmed ───────────────────────────────────────────────
        confirmed_payload = OrderConfirmedPayload(
            restaurant_id=restaurant.restaurant_id,
            estimated_prep_minutes=restaurant.avg_prep_minutes,
        ).model_dump()
        events.append(_build_event(EventType.ORDER_CONFIRMED, order_id, city, confirmed_payload))

        # ── Possible cancellation after confirmation ──────────────────────────
        if random.random() < restaurant.cancellation_rate:
            cancelled_payload = OrderCancelledPayload(
                reason=random.choice(["customer_cancelled", "restaurant_busy"]),
                cancelled_by=random.choice(["customer", "restaurant"]),
                refund_amount=order_value,
            ).model_dump()
            events.append(_build_event(EventType.ORDER_CANCELLED, order_id, city, cancelled_payload))
            return events

        # ── 4. order_preparing ───────────────────────────────────────────────
        preparing_payload = OrderPreparingPayload(
            restaurant_id=restaurant.restaurant_id,
            actual_prep_start_ts=_utcnow_iso(),
        ).model_dump()
        events.append(_build_event(EventType.ORDER_PREPARING, order_id, city, preparing_payload))

        # ── Possible cancellation during prep ────────────────────────────────
        if random.random() < 0.02:
            cancelled_payload = OrderCancelledPayload(
                reason="customer_cancelled",
                cancelled_by="customer",
                refund_amount=order_value,
            ).model_dump()
            events.append(_build_event(EventType.ORDER_CANCELLED, order_id, city, cancelled_payload))
            return events

        # ── 5. order_picked_up ───────────────────────────────────────────────
        rider = self.master.random_rider(city)
        pickup_payload = OrderPickedUpPayload(
            rider_id=rider.rider_id,
            restaurant_id=restaurant.restaurant_id,
            pickup_ts=_utcnow_iso(),
        ).model_dump()
        events.append(_build_event(EventType.ORDER_PICKED_UP, order_id, city, pickup_payload))

        # ── 6. order_delivered ───────────────────────────────────────────────
        delivery_payload = OrderDeliveredPayload(
            rider_id=rider.rider_id,
            customer_id=customer.customer_id,
            delivery_ts=_utcnow_iso(),
            distance_km=round(random.uniform(0.5, 15.0), 2),
            tip_amount=round(random.uniform(0, 50), 2) if random.random() < 0.3 else None,
        ).model_dump(exclude_none=True)
        events.append(_build_event(EventType.ORDER_DELIVERED, order_id, city, delivery_payload))

        return events


# ── Kafka Producer ────────────────────────────────────────────────────────────

def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=5,
        compression_type="lz4",
        linger_ms=5,
        batch_size=65536,
    )


def produce_events(
    producer: KafkaProducer,
    events: list[dict[str, Any]],
    topic: str,
    chaos_injector: ChaosInjector,
) -> tuple[int, int]:
    """Produce events to Kafka, applying chaos. Returns (sent, skipped)."""
    sent = 0
    skipped = 0
    for event in events:
        chaos_events = chaos_injector.inject(event)
        for ce in chaos_events:
            try:
                producer.send(
                    topic=topic,
                    key=ce["order_id"],
                    value=ce,
                )
                sent += 1
            except KafkaError as e:
                log.error("kafka.produce_error", error=str(e))
                skipped += 1
    return sent, skipped


# ── Restaurant master data producer ──────────────────────────────────────────

def produce_restaurant_master(
    producer: KafkaProducer,
    master: MasterData,
    topic: str,
    batch_size: int = 10,
) -> None:
    """Periodically emit restaurant master events (simulates SCD2 changes)."""
    sample = random.sample(master.restaurants, min(batch_size, len(master.restaurants)))
    for r in sample:
        # Occasionally mutate a field (name, tier) to drive SCD2
        if random.random() < 0.1:
            r.commission_tier = random.choice(["standard", "premium", "enterprise"])
        if random.random() < 0.05:
            r.name = r.name + " (Updated)"

        event = RestaurantMasterEvent(
            restaurant_id=r.restaurant_id,
            name=r.name,
            city=r.city,
            cuisine=r.cuisine,
            commission_tier=r.commission_tier,
            is_active=r.is_active,
            updated_ts=_utcnow_iso(),
        )
        producer.send(
            topic=topic,
            key=r.restaurant_id,
            value=json.loads(event.model_dump_json()),
        )


# ── Main loop ─────────────────────────────────────────────────────────────────

def run(config: dict[str, Any]) -> None:
    kafka_bootstrap = config["kafka"]["bootstrap"]
    orders_topic = config["kafka"].get("topic_orders", "orders.events")
    restaurants_topic = config["kafka"].get("topic_restaurants", "restaurants.master")

    target_eps = int(config["simulator"]["events_per_second"])
    burst_mode = config["simulator"].get("burst_mode", False)

    chaos_cfg = ChaosConfig(
        duplicate_rate=float(config["chaos"]["duplicate_rate"]),
        late_event_rate=float(config["chaos"]["late_event_rate"]),
        out_of_order_rate=float(config["chaos"]["out_of_order_rate"]),
        schema_drift_rate=float(config["chaos"]["schema_drift_rate"]),
        bad_value_rate=float(config["chaos"]["bad_value_rate"]),
        chaos_mode=bool(config["chaos"]["chaos_mode"]),
    )

    log.info("simulator.starting", kafka=kafka_bootstrap, target_eps=target_eps)

    producer = build_producer(kafka_bootstrap)
    injector = ChaosInjector(chaos_cfg)
    master = MasterData.get()
    order_sim = OrderSimulator(master)

    total_sent = 0
    total_orders = 0
    start_time = time.time()
    restaurant_master_counter = 0

    while True:
        loop_start = time.time()
        multiplier = get_load_multiplier(target_eps, burst_mode)
        effective_eps = max(1, int(target_eps * multiplier))

        # Generate and send one batch of orders
        batch_events: list[dict[str, Any]] = []
        batch_orders = max(1, effective_eps // 6)  # ~6 events per order on average

        for _ in range(batch_orders):
            order_events = order_sim.generate_order_events()

            # Out-of-order injection: shuffle event list
            if random.random() < chaos_cfg.out_of_order_rate and len(order_events) > 1:
                random.shuffle(order_events)

            batch_events.extend(order_events)

        sent, _ = produce_events(producer, batch_events, orders_topic, injector)
        producer.flush()

        total_sent += sent
        total_orders += batch_orders
        restaurant_master_counter += 1

        # Every ~100 batches emit restaurant master events
        if restaurant_master_counter % 100 == 0:
            produce_restaurant_master(producer, master, restaurants_topic)
            producer.flush()

        elapsed = time.time() - start_time
        if int(elapsed) % 10 == 0 and elapsed > 1:
            log.info(
                "simulator.stats",
                total_sent=total_sent,
                total_orders=total_orders,
                avg_eps=round(total_sent / elapsed, 1),
                current_multiplier=round(multiplier, 2),
            )

        # Rate limiting
        loop_elapsed = time.time() - loop_start
        sleep_time = max(0, 1.0 - loop_elapsed)
        time.sleep(sleep_time)


def benchmark_throughput(duration_seconds: int = 30) -> None:
    """Measure maximum producer throughput without Kafka (in-memory only)."""
    log.info("benchmark.start", duration_seconds=duration_seconds)
    master = MasterData.get()
    order_sim = OrderSimulator(master)
    count = 0
    start = time.time()

    while time.time() - start < duration_seconds:
        events = order_sim.generate_order_events()
        count += len(events)

    elapsed = time.time() - start
    log.info(
        "benchmark.result",
        events_generated=count,
        duration_s=round(elapsed, 2),
        events_per_sec=round(count / elapsed, 1),
    )


if __name__ == "__main__":
    cfg = load_config()
    run(cfg)
