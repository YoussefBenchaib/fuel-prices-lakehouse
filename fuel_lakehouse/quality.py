"""Parsing and data quality checks: bronze lines in, (valid rows, rejected rows) out.

Pure DataFrame-to-DataFrame functions with no I/O, so they can be tested on a few rows.
Every input line ends up in exactly one of the two outputs, with the reason if rejected.
"""

from decimal import Decimal

from pyspark.sql import Window
from pyspark.sql import functions as F

from fuel_lakehouse.raw import PRICE_COLUMNS, STATION_COLUMNS

MIN_PRICE = Decimal("0.500")
MAX_PRICE = Decimal("4.000")
# A price not updated for this long is kept, but flagged and left out of averages.
STALE_AFTER_DAYS = 7


def split_fields(lines, columns, free_text_column=None):
    """Splits '|'-separated lines into string columns named like the source header.

    Some station names contain '|' themselves ("STOIL SIMPLE | gestori.prezzibenzina.it").
    When exactly one extra '|' appears, it can only belong to free_text_column if the fields
    before it are read from the left and the fields after it from the right. Anything else is
    ambiguous, and the row is marked as not well formed rather than guessed.
    """
    parts = F.split("line", r"\|", -1)
    n = F.size(parts)
    expected = len(columns)

    if free_text_column is None:
        well_formed = n == expected
        fields = [parts.getItem(i) for i in range(expected)]
    else:
        idx = columns.index(free_text_column)
        well_formed = (n == expected) | (n == expected + 1)
        fields = []
        for i in range(expected):
            if i < idx:
                fields.append(parts.getItem(i))
            elif i == idx:
                fields.append(F.when(n == expected + 1,
                                     F.concat_ws("|", parts.getItem(i), parts.getItem(i + 1)))
                              .otherwise(parts.getItem(i)))
            else:
                # Negative index: counted from the end of the line.
                fields.append(F.element_at(parts, i - expected))

    return lines.select(
        "*",
        well_formed.alias("well_formed"),
        *[F.trim(field).alias(name) for field, name in zip(fields, columns)],
    )


def check_stations(lines):
    df = split_fields(lines, STATION_COLUMNS, free_text_column="Nome Impianto")
    df = df.select(
        "line", "extraction_date", "well_formed",
        F.expr("try_cast(idImpianto AS INT)").alias("station_id"),
        F.col("Gestore").alias("operator"),
        F.col("Bandiera").alias("brand"),
        F.col("`Tipo Impianto`").alias("station_type"),
        F.col("`Nome Impianto`").alias("name"),
        F.col("Indirizzo").alias("address"),
        F.col("Comune").alias("municipality"),
        F.col("Provincia").alias("province"),
        F.expr("try_cast(Latitudine AS DOUBLE)").alias("latitude"),
        F.expr("try_cast(Longitudine AS DOUBLE)").alias("longitude"),
    )
    same_id = F.count("*").over(Window.partitionBy("extraction_date", "station_id"))
    reason = (
        F.when(~F.col("well_formed"), "malformed_row")
        .when(F.col("station_id").isNull(), "invalid_station_id")
        .when(same_id > 1, "duplicate_station_id")
    )
    df = df.withColumn("reject_reason", reason)

    valid = df.where(F.col("reject_reason").isNull()).drop("line", "well_formed", "reject_reason")
    rejected = df.where(F.col("reject_reason").isNotNull()).select(
        "extraction_date", "line", "reject_reason")
    return valid, rejected


def check_prices(lines, valid_stations):
    df = split_fields(lines, PRICE_COLUMNS)
    df = df.select(
        "line", "extraction_date", "well_formed",
        F.expr("try_cast(idImpianto AS INT)").alias("station_id"),
        F.col("descCarburante").alias("fuel"),
        # Decimal, not double: 2.349 stays exactly 2.349.
        F.expr("try_cast(prezzo AS DECIMAL(6,3))").alias("price"),
        F.when(F.col("isSelf") == "1", True).when(F.col("isSelf") == "0", False).alias("is_self"),
        F.expr("try_to_timestamp(dtComu, 'dd/MM/yyyy HH:mm:ss')").alias("reported_at"),
    )

    known = valid_stations.select(
        "extraction_date", "station_id", F.lit(True).alias("station_known")).distinct()
    df = df.join(known, ["extraction_date", "station_id"], "left")

    same_key = F.count("*").over(
        Window.partitionBy("extraction_date", "station_id", "fuel", "is_self"))
    next_day = F.date_add("extraction_date", 1).cast("timestamp")
    reason = (
        F.when(~F.col("well_formed"), "malformed_row")
        .when(F.col("station_id").isNull(), "invalid_station_id")
        .when(F.col("fuel") == "", "missing_fuel")
        .when(F.col("price").isNull(), "invalid_price")
        .when((F.col("price") < MIN_PRICE) | (F.col("price") > MAX_PRICE), "price_out_of_range")
        .when(F.col("is_self").isNull(), "invalid_is_self")
        .when(F.col("reported_at").isNull(), "invalid_timestamp")
        .when(F.col("reported_at") >= next_day, "reported_in_future")
        .when(same_key > 1, "duplicate_price")
        .when(F.col("station_known").isNull(), "unknown_station")
    )
    df = df.withColumn("reject_reason", reason)

    stale_before = F.date_sub("extraction_date", STALE_AFTER_DAYS).cast("timestamp")
    valid = (
        df.where(F.col("reject_reason").isNull())
        .withColumn("is_stale", F.col("reported_at") < stale_before)
        .select("extraction_date", "station_id", "fuel", "is_self", "price",
                "reported_at", "is_stale")
    )
    rejected = df.where(F.col("reject_reason").isNotNull()).select(
        "extraction_date", "line", "reject_reason")
    return valid, rejected
