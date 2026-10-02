"""
spark_session.py — Factory for creating a SparkSession configured for:
  - Delta Lake (DeltaSparkSessionExtension + DeltaCatalog)
  - MinIO / S3A (Hadoop-AWS with endpoint override)
  - Structured Streaming defaults

Usage:
    from common.spark_session import get_spark
    spark = get_spark("BronzeIngest")
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession


def get_spark(app_name: str, shuffle_partitions: int | None = None) -> SparkSession:
    """
    Build and return a SparkSession with Delta Lake + S3A (MinIO) config.

    Args:
        app_name: Name shown in the Spark UI.
        shuffle_partitions: Override spark.sql.shuffle.partitions.
                            Defaults to SPARK_SHUFFLE_PARTITIONS env var or 8.
    """
    n_partitions = shuffle_partitions or int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))

    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")

    spark = (
        SparkSession.builder.appName(app_name)
        # ── Delta Lake extensions ──────────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ── S3A / MinIO ────────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", aws_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # ── Streaming defaults ─────────────────────────────────────────────
        .config("spark.sql.shuffle.partitions", str(n_partitions))
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # ── Delta write performance ────────────────────────────────────────
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128 MB
        # ── Logging verbosity ─────────────────────────────────────────────
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
