"""
Peek at the first N raw rows of a Bronze file - exactly as stored, no cleaning.

Run:  python peek.py                  (defaults to 2024-01, 100 rows)
      python peek.py 2025 03 50       (year, month, rows)
"""
import sys
from pathlib import Path

import pandas as pd

from spark_session import PROJECT_ROOT, get_spark, s3_path

year = sys.argv[1] if len(sys.argv) > 1 else "2024"
month = sys.argv[2] if len(sys.argv) > 2 else "01"
n = int(sys.argv[3]) if len(sys.argv) > 3 else 100

FILE = f"yellow_tripdata_{year}-{month}.parquet"
OUT = PROJECT_ROOT / "reports" / f"peek_{year}-{month}.csv"

spark = get_spark("tlc-peek")

# limit() lets Spark stop after the first row group - it doesn't scan the whole file
pdf = spark.read.parquet(s3_path(year, month, FILE)).limit(n).toPandas()

# Show every column, full width, no "..." truncation
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_rows", None)

print(pdf.to_string(index=False))
print(f"\n{len(pdf)} rows x {len(pdf.columns)} columns from {FILE}")

# Also save it - easier to scroll in VS Code or Excel than in the terminal
OUT.parent.mkdir(exist_ok=True)
pdf.to_csv(OUT, index=False)
print(f"Saved to {OUT}")

spark.stop()