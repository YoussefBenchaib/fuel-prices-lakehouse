"""Processes one day of raw files into the lake: bronze, quarantine, silver, gold, report.

    data/lake/
      bronze/prices/extraction_date=2026-10-20/       raw lines, exactly as received
      bronze/stations/...
      quarantine/prices/...                            rejected lines, with the reason
      quarantine/stations/...
      silver/prices/...                                clean, typed, joined with stations
      gold/province_prices/...                         daily averages per province and fuel
      _reports/2026-10-20.json                         row counts and reasons
"""

import json
import os

from pyspark.sql import functions as F

from fuel_lakehouse import quality, raw, transform


class ReconciliationError(Exception):
    """Rows were lost or duplicated between bronze and silver + quarantine."""


def write_partitioned(df, path):
    # With dynamic partition overwrite, only this day's folder is replaced: re-running is safe.
    df.write.mode("overwrite").partitionBy("extraction_date").parquet(path)


def read_day(spark, path, extraction_date):
    # Filtering on the partition column makes Spark open only that day's folder.
    return spark.read.parquet(path).where(F.col("extraction_date") == extraction_date)


def reconcile(name, bronze_count, valid_count, rejected_count):
    if bronze_count != valid_count + rejected_count:
        raise ReconciliationError(
            f"{name}: {bronze_count} lines in bronze, but {valid_count} valid "
            f"+ {rejected_count} rejected = {valid_count + rejected_count}")


def count_reasons(rejected):
    return {row["reject_reason"]: row["count"]
            for row in rejected.groupBy("reject_reason").count().collect()}


def process_day(spark, day_dir, lake_dir):
    lake = lambda *parts: os.path.join(lake_dir, *parts)

    prices_date, price_lines = raw.read_lines(
        spark, os.path.join(day_dir, raw.PRICES_FILE), raw.PRICE_COLUMNS)
    stations_date, station_lines = raw.read_lines(
        spark, os.path.join(day_dir, raw.STATIONS_FILE), raw.STATION_COLUMNS)
    if prices_date != stations_date:
        raise ValueError(f"prices from {prices_date}, stations from {stations_date}")
    day = prices_date

    # Bronze: store first, then work from what was stored.
    write_partitioned(price_lines, lake("bronze", "prices"))
    write_partitioned(station_lines, lake("bronze", "stations"))
    bronze_prices = read_day(spark, lake("bronze", "prices"), day)
    bronze_stations = read_day(spark, lake("bronze", "stations"), day)

    valid_stations, rejected_stations = quality.check_stations(bronze_stations)
    valid_stations = valid_stations.cache()
    valid_prices, rejected_prices = quality.check_prices(bronze_prices, valid_stations)
    valid_prices = valid_prices.cache()

    counts = {
        "stations": {"bronze": bronze_stations.count(), "valid": valid_stations.count(),
                     "rejected": rejected_stations.count()},
        "prices": {"bronze": bronze_prices.count(), "valid": valid_prices.count(),
                   "rejected": rejected_prices.count()},
    }
    for name, c in counts.items():
        reconcile(name, c["bronze"], c["valid"], c["rejected"])

    silver = transform.silver_prices(valid_prices, valid_stations)
    write_partitioned(rejected_stations, lake("quarantine", "stations"))
    write_partitioned(rejected_prices, lake("quarantine", "prices"))
    write_partitioned(silver, lake("silver", "prices"))
    write_partitioned(transform.gold_province_prices(silver), lake("gold", "province_prices"))

    report = {
        "extraction_date": day.isoformat(),
        "stations": {**counts["stations"], "reasons": count_reasons(rejected_stations)},
        "prices": {**counts["prices"], "reasons": count_reasons(rejected_prices),
                   "stale": valid_prices.where("is_stale").count()},
    }
    os.makedirs(lake("_reports"), exist_ok=True)
    with open(lake("_reports", f"{day.isoformat()}.json"), "w") as f:
        json.dump(report, f, indent=2)

    valid_stations.unpersist()
    valid_prices.unpersist()
    return report
