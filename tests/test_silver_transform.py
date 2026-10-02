"""
test_silver_transform.py — Unit tests for Silver validation and transformation logic.

Tests the pure Python logic extracted from silver_transform.py,
using small in-memory DataFrames (Spark local mode).
"""
from __future__ import annotations

import pytest

# Mark all tests in this module as integration (require PySpark)
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark():
    """Create a local SparkSession for testing (no cluster needed)."""
    try:
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("SilverTransformTests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        yield spark
        spark.stop()
    except ImportError:
        pytest.skip("PySpark not installed")


@pytest.fixture
def valid_events_df(spark):
    """Small DataFrame with valid events."""
    data = [
        ("evt-001", "order_placed",    1, "2026-10-02T12:00:00.000Z", "ORD-001", "Bengaluru", None),
        ("evt-002", "order_confirmed", 1, "2026-10-02T12:01:00.000Z", "ORD-001", "Bengaluru", None),
        ("evt-003", "order_delivered", 1, "2026-10-02T12:45:00.000Z", "ORD-001", "Bengaluru", None),
        ("evt-004", "order_placed",    1, "2026-10-02T12:05:00.000Z", "ORD-002", "Mumbai",    None),
    ]
    return spark.createDataFrame(
        data,
        ["event_id", "event_type", "schema_version", "event_ts", "order_id", "city", "raw_json"]
    )


@pytest.fixture
def bad_events_df(spark):
    """Small DataFrame with known-bad events."""
    data = [
        (None,      "order_placed",      1, "2026-10-02T12:00:00.000Z", "ORD-003", "Delhi",   None),  # null event_id
        ("evt-005", "order_teleported",  1, "2026-10-02T12:00:00.000Z", "ORD-004", "Chennai", None),  # invalid type
        ("evt-006", "order_placed",      1, "2026-10-02T12:00:00.000Z", "ORD-005", None,       None),  # null city
        ("evt-007", "order_placed",      9, "2026-10-02T12:00:00.000Z", "ORD-006", "Pune",    None),  # bad version
        ("evt-008", "order_placed",      1, "not-a-timestamp",           "ORD-007", "Kolkata", None),  # bad ts
    ]
    return spark.createDataFrame(
        data,
        ["event_id", "event_type", "schema_version", "event_ts", "order_id", "city", "raw_json"]
    )


class TestValidation:
    def test_clean_events_pass_validation(self, valid_events_df):
        from streaming.silver_transform import validate_and_split
        clean, bad = validate_and_split(valid_events_df)
        assert clean.count() == 4
        assert bad.count() == 0

    def test_null_event_id_goes_to_dlq(self, bad_events_df):
        from streaming.silver_transform import validate_and_split
        clean, bad = validate_and_split(bad_events_df)
        null_id_bad = bad.filter(bad["error_reason"] == "null_event_id")
        assert null_id_bad.count() == 1

    def test_invalid_event_type_goes_to_dlq(self, bad_events_df):
        from streaming.silver_transform import validate_and_split
        _, bad = validate_and_split(bad_events_df)
        bad_type = bad.filter(bad["error_reason"].startswith("invalid_event_type"))
        assert bad_type.count() == 1

    def test_null_city_goes_to_dlq(self, bad_events_df):
        from streaming.silver_transform import validate_and_split
        _, bad = validate_and_split(bad_events_df)
        null_city = bad.filter(bad["error_reason"] == "null_city")
        assert null_city.count() == 1

    def test_unsupported_schema_version_goes_to_dlq(self, bad_events_df):
        from streaming.silver_transform import validate_and_split
        _, bad = validate_and_split(bad_events_df)
        bad_version = bad.filter(bad["error_reason"].startswith("unsupported_schema_version"))
        assert bad_version.count() == 1

    def test_all_five_bad_events_caught(self, bad_events_df):
        from streaming.silver_transform import validate_and_split
        clean, bad = validate_and_split(bad_events_df)
        assert bad.count() == 5
        assert clean.count() == 0

    def test_dlq_has_required_columns(self, bad_events_df):
        from streaming.silver_transform import validate_and_split
        _, bad = validate_and_split(bad_events_df)
        required = {"raw_event", "error_reason", "error_stage", "quarantined_at"}
        assert required.issubset(set(bad.columns))


class TestDeduplication:
    def test_latest_per_key_keeps_one_row(self, spark):
        from streaming.common.delta_utils import latest_per_key

        data = [
            ("ORD-001", "2026-10-02T12:00:00"),
            ("ORD-001", "2026-10-02T12:05:00"),   # later — should be kept
            ("ORD-001", "2026-10-02T11:55:00"),
            ("ORD-002", "2026-10-02T12:01:00"),
        ]
        df = spark.createDataFrame(data, ["order_id", "event_ts"])
        result = latest_per_key(df, "order_id", "event_ts")

        assert result.count() == 2

        ord_001_ts = result.filter(result.order_id == "ORD-001").collect()[0]["event_ts"]
        assert "12:05:00" in str(ord_001_ts)

    def test_dedup_handles_single_row(self, spark):
        from streaming.common.delta_utils import latest_per_key

        data = [("ORD-999", "2026-10-02T12:00:00")]
        df = spark.createDataFrame(data, ["order_id", "event_ts"])
        result = latest_per_key(df, "order_id", "event_ts")
        assert result.count() == 1
