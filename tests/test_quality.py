import datetime
from decimal import Decimal

from conftest import price, station
from fuel_lakehouse import quality

DAY = datetime.date(2026, 10, 20)


def lines(spark, rows):
    return spark.createDataFrame([(r, DAY) for r in rows], "line string, extraction_date date")


def reasons(rejected):
    return sorted(r["reject_reason"] for r in rejected.collect())


def test_station_name_containing_the_separator_is_rejoined(spark):
    valid, rejected = quality.check_stations(lines(spark, [
        station(1, name="STOIL SIMPLE | gestori.prezzibenzina.it"),
    ]))
    row = valid.collect()[0]
    assert row["name"] == "STOIL SIMPLE | gestori.prezzibenzina.it"
    assert row["municipality"] == "MILANO"
    assert row["province"] == "MI"
    assert rejected.count() == 0


def test_station_with_ambiguous_separators_goes_to_quarantine(spark):
    ambiguous = "54386|PRADELLI | x|Pompe Bianche|Stradale|PRADELLI | x|VIA 1|ZOCCA|MO|44.3|11.0"
    valid, rejected = quality.check_stations(lines(spark, [ambiguous, station(2)]))
    assert [r["station_id"] for r in valid.collect()] == [2]
    assert reasons(rejected) == ["malformed_row"]


def test_duplicate_and_invalid_station_ids_are_rejected(spark):
    valid, rejected = quality.check_stations(lines(spark, [
        station(1), station(1), station("abc"), station(2)]))
    assert [r["station_id"] for r in valid.collect()] == [2]
    assert reasons(rejected) == ["duplicate_station_id", "duplicate_station_id",
                                 "invalid_station_id"]


def test_price_checks(spark):
    stations, _ = quality.check_stations(lines(spark, [station(1), station(2), station(3)]))
    valid, rejected = quality.check_prices(lines(spark, [
        price(1),                                      # valid
        price(1, fuel="Gasolio", value="0.100"),       # price_out_of_range
        price(1, fuel="GPL", value="8.888"),           # price_out_of_range
        price(1, fuel="Metano", value="n/a"),          # invalid_price
        price(2, is_self="2"),                         # invalid_is_self
        price(2, fuel="Gasolio", reported="ieri"),     # invalid_timestamp
        price(2, fuel="GPL", reported="21/10/2026 00:00:00"),  # reported_in_future
        price(3), price(3),                            # duplicate_price x2
        price(99),                                     # unknown_station
        "1|Benzina|2.000",                             # malformed_row
    ]), stations)
    assert valid.count() == 1
    assert reasons(rejected) == [
        "duplicate_price", "duplicate_price", "invalid_is_self", "invalid_price",
        "invalid_timestamp", "malformed_row", "price_out_of_range", "price_out_of_range",
        "reported_in_future", "unknown_station",
    ]


def test_price_is_exact_decimal(spark):
    stations, _ = quality.check_stations(lines(spark, [station(1)]))
    valid, _ = quality.check_prices(lines(spark, [price(1, value="2.349")]), stations)
    assert valid.collect()[0]["price"] == Decimal("2.349")


def test_old_prices_are_kept_but_flagged_stale(spark):
    stations, _ = quality.check_stations(lines(spark, [station(1)]))
    valid, rejected = quality.check_prices(lines(spark, [
        price(1, fuel="Benzina", reported="13/10/2026 00:00:00"),  # exactly 7 days: fresh
        price(1, fuel="Gasolio", reported="12/10/2026 23:59:59"),  # older: stale
        price(1, fuel="GPL", reported="23/05/2013 01:03:09"),      # real case in the data
    ]), stations)
    stale = {r["fuel"]: r["is_stale"] for r in valid.collect()}
    assert stale == {"Benzina": False, "Gasolio": True, "GPL": True}
    assert rejected.count() == 0
