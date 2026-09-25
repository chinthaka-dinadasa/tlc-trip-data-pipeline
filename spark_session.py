"""
Shared SparkSession factory for the TLC pipeline.

Every later step (schema inventory, silver, gold) imports get_spark()
so the MinIO / S3A config lives in exactly one place.
"""
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent

# Load .env BEFORE the JVM starts, so SPARK_LOCAL_IP and the MinIO keys
# are inherited by the Java process PySpark launches.
load_dotenv(PROJECT_ROOT / ".env")

JARS = [
    PROJECT_ROOT / "jars" / "hadoop-aws-3.3.4.jar",              # provides S3AFileSystem (s3a://)
    PROJECT_ROOT / "jars" / "aws-java-sdk-bundle-1.12.262.jar",  # HTTP client hadoop-aws uses
]

BUCKET = os.environ.get("TLC_BUCKET", "tlc-trip-records")


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} is not set - check .env in {PROJECT_ROOT}")
    return value


def get_spark(app_name: str = "tlc", driver_memory: str = "8g") -> SparkSession:
    missing = [str(j) for j in JARS if not j.exists()]
    if missing:
        raise FileNotFoundError(f"Missing S3A jars: {missing}")

    spark = (
        SparkSession.builder.appName(app_name)
        # local[*] = driver and executor in one JVM, one task thread per core
        .master("local[*]")
        # Absolute paths, so it works no matter which folder you run from
        .config("spark.jars", ",".join(str(j) for j in JARS))

        # --- MinIO via S3A ---
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", _require_env("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", _require_env("MINIO_SECRET_KEY"))
        # MinIO serves host:9000/bucket, not AWS-style bucket.host
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # --- Keep all internal Spark traffic on loopback (the VS Code / LAN issue) ---
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")

        # --- Local-mode sizing ---
        # In local mode the driver IS the cluster; the 1g default will OOM
        .config("spark.driver.memory", driver_memory)
        # Default 200 is tuned for clusters; 2-4x core count suits one machine
        .config("spark.sql.shuffle.partitions", "32")
        # Timestamps shown in UTC, matching how TLC data is compared across months
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def s3_path(*parts: str) -> str:
    """s3_path('2024', '01', 'yellow_tripdata_2024-01.parquet') -> s3a://tlc-trip-records/2024/01/..."""
    return "s3a://" + "/".join([BUCKET, *parts])