"""Silver and gold tables, built from rows that passed the quality checks."""

from pyspark.sql import functions as F


def silver_prices(valid_prices, valid_stations):
    """One row per station, fuel and service mode, with the station's details."""
    stations = valid_stations.select(
        "extraction_date", "station_id", "brand", "station_type", "name",
        "municipality", "province", "latitude", "longitude")
    return valid_prices.join(stations, ["extraction_date", "station_id"], "inner")


def gold_province_prices(silver):
    """Daily price statistics per province, fuel and self/served. Stale prices are left out."""
    return (
        silver.where(~F.col("is_stale"))
        .groupBy("extraction_date", "province", "fuel", "is_self")
        .agg(
            F.countDistinct("station_id").alias("stations"),
            F.round(F.avg("price"), 3).cast("decimal(6,3)").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
        )
    )
