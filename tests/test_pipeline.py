import json
import os

import pytest
from pyspark.sql import functions as F

from conftest import price, station
from fuel_lakehouse import pipeline
from fuel_lakehouse.raw import SchemaChangedError, parse_extraction_date


def test_parse_extraction_date():
    assert parse_extraction_date("Estrazione del 2026-10-20\n").isoformat() == "2026-10-20"
    with pytest.raises(ValueError):
        parse_extraction_date("idImpianto|descCarburante")


def test_every_line_ends_up_in_silver_or_quarantine(spark, write_day, lake_dir):
    day = write_day(
        prices=[price(1, value="2.000"), price(2, value="2.200"), price(1, fuel="GPL",
                value="0.100")],
        stations=[station(1), station(2)],
    )
    report = pipeline.process_day(spark, day, lake_dir)

    assert report["prices"] == {"bronze": 3, "valid": 2, "rejected": 1,
                                "reasons": {"price_out_of_range": 1}, "stale": 0}
    assert spark.read.parquet(os.path.join(lake_dir, "silver", "prices")).count() == 2
    assert spark.read.parquet(os.path.join(lake_dir, "quarantine", "prices")).count() == 1
    with open(os.path.join(lake_dir, "_reports", "2026-10-20.json")) as f:
        assert json.load(f) == report


def test_gold_averages_leave_out_stale_prices(spark, write_day, lake_dir):
    day = write_day(
        prices=[price(1, value="2.000"), price(2, value="2.100"),
                price(3, value="3.000", reported="01/01/2026 08:00:00")],
        stations=[station(1), station(2), station(3)],
    )
    pipeline.process_day(spark, day, lake_dir)

    gold = spark.read.parquet(os.path.join(lake_dir, "gold", "province_prices")).collect()
    assert len(gold) == 1
    assert gold[0]["stations"] == 2
    assert str(gold[0]["avg_price"]) == "2.050"
    assert str(gold[0]["max_price"]) == "2.100"


def test_rerunning_a_day_replaces_it_and_keeps_other_days(spark, write_day, lake_dir):
    pipeline.process_day(spark, write_day([price(1)], [station(1)], date="2026-10-19"),
                         lake_dir)
    day_20 = write_day([price(1), price(2)], [station(1), station(2)], date="2026-10-20")
    pipeline.process_day(spark, day_20, lake_dir)
    pipeline.process_day(spark, day_20, lake_dir)

    silver = spark.read.parquet(os.path.join(lake_dir, "silver", "prices"))
    per_day = {str(r["extraction_date"]): r["count"]
               for r in silver.groupBy("extraction_date").count().collect()}
    assert per_day == {"2026-10-19": 1, "2026-10-20": 2}


def test_changed_header_stops_the_pipeline(spark, write_day, lake_dir):
    day = write_day([price(1)], [station(1)],
                    prices_header="idImpianto|descCarburante|prezzo|isSelf|dtComu|nuovaColonna")
    with pytest.raises(SchemaChangedError):
        pipeline.process_day(spark, day, lake_dir)
    assert not os.path.exists(lake_dir)


def test_reconcile_detects_lost_rows():
    pipeline.reconcile("prices", 10, 7, 3)
    with pytest.raises(pipeline.ReconciliationError):
        pipeline.reconcile("prices", 10, 7, 2)


def test_reading_one_day_only_opens_its_partition(spark, write_day, lake_dir):
    pipeline.process_day(spark, write_day([price(1)], [station(1)], date="2026-10-19"),
                         lake_dir)
    pipeline.process_day(spark, write_day([price(1)], [station(1)], date="2026-10-20"),
                         lake_dir)
    df = pipeline.read_day(spark, os.path.join(lake_dir, "silver", "prices"),
                           F.lit("2026-10-20").cast("date"))
    plan = df._jdf.queryExecution().executedPlan().toString()
    assert "PartitionFilters: [isnotnull(extraction_date" in plan
    assert df.count() == 1
