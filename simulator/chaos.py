"""
chaos.py — Chaos injection for the food delivery event simulator.

Injects configurable rates of:
  - Duplicates      : re-send the same event_id
  - Late events     : shift event_ts backwards by 1-15 minutes
  - Out-of-order    : return events in wrong lifecycle sequence
  - Schema drift    : add/remove/rename fields; bump schema_version
  - Bad values      : negative order_value, null city, impossible timestamps
  - Load spikes     : simulated lunch/dinner traffic bursts
"""
from __future__ import annotations

import copy
import random
from datetime import datetime, timedelta, timezone

import structlog

log = structlog.get_logger(__name__)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _shift_ts(iso_str: str, delta_minutes: float) -> str:
    """Shift an ISO-8601 timestamp by delta_minutes (can be negative for late events)."""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    shifted = dt + timedelta(minutes=delta_minutes)
    return shifted.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class ChaosConfig:
    """Holds all chaos rates. Loaded from config.yaml / env vars."""

    def __init__(
        self,
        duplicate_rate: float = 0.02,
        late_event_rate: float = 0.03,
        out_of_order_rate: float = 0.02,
        schema_drift_rate: float = 0.005,
        bad_value_rate: float = 0.01,
        chaos_mode: bool = True,
    ) -> None:
        self.duplicate_rate = duplicate_rate
        self.late_event_rate = late_event_rate
        self.out_of_order_rate = out_of_order_rate
        self.schema_drift_rate = schema_drift_rate
        self.bad_value_rate = bad_value_rate
        self.chaos_mode = chaos_mode

    @classmethod
    def from_dict(cls, d: dict) -> ChaosConfig:
        return cls(**{k: v for k, v in d.items() if k in cls.__init__.__code__.co_varnames})


class ChaosInjector:
    """
    Wraps an event dict and applies chaos based on configured rates.

    Usage:
        injector = ChaosInjector(config)
        events = injector.inject(event_dict)  # returns list (usually 1, 2 if duplicate)
    """

    def __init__(self, config: ChaosConfig) -> None:
        self.config = config

    def inject(self, event: dict) -> list[dict]:
        """
        Apply chaos to an event. Returns a list of events:
          - Normally: [event]
          - Duplicate: [event, event_copy]   (same event_id, slight delay)
          - Filtered:  []                    (shouldn't happen, just in case)
        """
        if not self.config.chaos_mode:
            return [event]

        results = [event]

        # 1. Schema drift — modify payload structure
        if random.random() < self.config.schema_drift_rate:
            results = [self._inject_schema_drift(e) for e in results]
            log.debug("chaos.schema_drift", event_id=event.get("event_id"))

        # 2. Bad values — corrupt field values
        if random.random() < self.config.bad_value_rate:
            results = [self._inject_bad_value(e) for e in results]
            log.debug("chaos.bad_value", event_id=event.get("event_id"))

        # 3. Late event — backdate event_ts
        if random.random() < self.config.late_event_rate:
            results = [self._inject_late_event(e) for e in results]
            log.debug("chaos.late_event", event_id=event.get("event_id"))

        # 4. Duplicate — append a copy with the same event_id
        if random.random() < self.config.duplicate_rate:
            duplicate = copy.deepcopy(results[0])
            # Slightly different produced_ts to look like network retry
            duplicate["produced_ts"] = _shift_ts(duplicate["produced_ts"], delta_minutes=random.uniform(0.01, 0.5))
            results.append(duplicate)
            log.debug("chaos.duplicate", event_id=event.get("event_id"))

        return results

    # ── Chaos methods ────────────────────────────────────────────────────────

    def _inject_late_event(self, event: dict) -> dict:
        """Backdate event_ts by 1-15 minutes (mobile network lag)."""
        minutes = random.uniform(1, 15)
        event = copy.deepcopy(event)
        event["event_ts"] = _shift_ts(event["event_ts"], delta_minutes=-minutes)
        event["_chaos"] = event.get("_chaos", []) + ["late_event"]
        return event

    def _inject_schema_drift(self, event: dict) -> dict:
        """Simulate schema drift: add unexpected field or remove required field."""
        event = copy.deepcopy(event)
        drift_type = random.choice(["add_field", "bump_version", "remove_field"])

        if drift_type == "add_field":
            # v2 adds optional fields
            event["schema_version"] = 2
            event["payload"]["_extra_field"] = "drift_value_" + str(random.randint(1, 999))
        elif drift_type == "bump_version":
            event["schema_version"] = 3  # unsupported version — should go to DLQ
        elif drift_type == "remove_field":
            # Remove a usually-optional payload field
            event["payload"].pop("rider_id", None)
            event["payload"].pop("restaurant_id", None)

        event["_chaos"] = event.get("_chaos", []) + ["schema_drift"]
        return event

    def _inject_bad_value(self, event: dict) -> dict:
        """Inject bad field values that should be caught by Silver validation."""
        event = copy.deepcopy(event)
        bad_type = random.choice([
            "negative_order_value",
            "null_city",
            "impossible_timestamp",
            "null_event_type",
            "extreme_order_value",
        ])

        if bad_type == "negative_order_value":
            event["payload"]["order_value"] = round(random.uniform(-999, -0.01), 2)
        elif bad_type == "null_city":
            event["city"] = None  # type: ignore[assignment]
        elif bad_type == "impossible_timestamp":
            # Future timestamp 30 days ahead
            event["event_ts"] = _shift_ts(event["event_ts"], delta_minutes=43200)
        elif bad_type == "null_event_type":
            event["event_type"] = None  # type: ignore[assignment]
        elif bad_type == "extreme_order_value":
            event["payload"]["order_value"] = round(random.uniform(100000, 999999), 2)

        event["_chaos"] = event.get("_chaos", []) + ["bad_value"]
        return event


# ── Load pattern helpers ──────────────────────────────────────────────────────

def get_load_multiplier(base_rate: int, burst_mode: bool = False) -> float:
    """
    Returns a multiplier (0.1 – 3.0) based on time of day.
    Lunch peak: 12-14h, Dinner peak: 19-22h.
    """
    if burst_mode:
        return 3.0

    hour = datetime.now(timezone.utc).hour + 5.5  # rough IST offset
    hour = hour % 24

    # Lunch peak
    if 12 <= hour < 14:
        return random.uniform(2.0, 2.8)
    # Dinner peak
    if 19 <= hour < 22:
        return random.uniform(2.2, 3.0)
    # Late night / early morning — very quiet
    if 0 <= hour < 6:
        return random.uniform(0.05, 0.15)
    # Normal hours
    return random.uniform(0.6, 1.2)
