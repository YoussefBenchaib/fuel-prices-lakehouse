"""Command line:

    python -m fuel_lakehouse download --raw-dir data/raw
    python -m fuel_lakehouse process --raw-dir data/raw --lake-dir data/lake [--date 2026-10-20]
"""

import argparse
import json
import os

from fuel_lakehouse import pipeline, raw
from fuel_lakehouse.spark import get_spark


def main():
    parser = argparse.ArgumentParser(prog="fuel_lakehouse")
    sub = parser.add_subparsers(dest="command", required=True)

    download = sub.add_parser("download", help="save today's MIMIT files under raw-dir/<date>/")
    download.add_argument("--raw-dir", default="data/raw")

    process = sub.add_parser("process", help="load raw days into the lake")
    process.add_argument("--raw-dir", default="data/raw")
    process.add_argument("--lake-dir", default="data/lake")
    process.add_argument("--date", help="one day (YYYY-MM-DD); default: every day in raw-dir")

    args = parser.parse_args()

    if args.command == "download":
        print(f"saved extraction of {raw.download(args.raw_dir)} in {args.raw_dir}")
        return

    days = [args.date] if args.date else sorted(os.listdir(args.raw_dir))
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    for day in days:
        report = pipeline.process_day(spark, os.path.join(args.raw_dir, day), args.lake_dir)
        print(json.dumps(report, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
