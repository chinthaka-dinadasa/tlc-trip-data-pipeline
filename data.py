"""
Step 1 - verify Spark can read Bronze from MinIO.

Run from the project root:  python data.py
"""
import time

from spark_session import PROJECT_ROOT, get_spark, s3_path

YEAR, MONTH = "2024", "01"
FILE = f"yellow_tripdata_{YEAR}-{MONTH}.parquet"

# Local copy from the download script, used only for the DuckDB cross-check.
# Change this if you downloaded somewhere else.
LOCAL_COPY = PROJECT_ROOT.parent / "nyc-taxi" / YEAR / MONTH / FILE


def main() -> None:
    spark = get_spark("tlc-verify")
    print(f"Spark {spark.version} | UI: {spark.sparkContext.uiWebUrl}")

    path = s3_path(YEAR, MONTH, FILE)
    print(f"Reading {path}")

    t0 = time.time()
    df = spark.read.parquet(path)   # lazy: reads only the footer for the schema
    df.printSchema()

    n = df.count()                  # first action: this actually runs a job
    print(f"rows={n:,}  partitions={df.rdd.getNumPartitions()}  took={time.time() - t0:.1f}s")

    df.select("tpep_pickup_datetime", "PULocationID", "trip_distance", "total_amount") \
      .show(5, truncate=False)

    # Independent check: same file, different engine, no S3A involved
    if LOCAL_COPY.exists():
        import duckdb
        local_n = duckdb.sql(f"SELECT count(*) FROM '{LOCAL_COPY}'").fetchone()[0]
        status = "MATCH" if local_n == n else "MISMATCH"
        print(f"duckdb rows={local_n:,}  -> {status}")
    else:
        print(f"(skipping DuckDB check - no local copy at {LOCAL_COPY})")

    input("Spark UI is live - press Enter to exit...")
    spark.stop()


if __name__ == "__main__":
    main()