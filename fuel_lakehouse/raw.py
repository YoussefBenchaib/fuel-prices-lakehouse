"""Raw MIMIT files: download, header checks, and reading them as lines.

Each file starts with an extraction line and a header:

    Estrazione del 2026-10-20
    idImpianto|descCarburante|prezzo|isSelf|dtComu
    3464|Benzina|2.349|0|16/10/2026 21:00:07
"""

import datetime
import os
import re
import urllib.request

from pyspark.sql import functions as F

BASE_URL = "https://www.mimit.gov.it/images/exportCSV/"
PRICES_FILE = "prezzo_alle_8.csv"
STATIONS_FILE = "anagrafica_impianti_attivi.csv"

PRICE_COLUMNS = ["idImpianto", "descCarburante", "prezzo", "isSelf", "dtComu"]
STATION_COLUMNS = [
    "idImpianto", "Gestore", "Bandiera", "Tipo Impianto", "Nome Impianto",
    "Indirizzo", "Comune", "Provincia", "Latitudine", "Longitudine",
]


class SchemaChangedError(Exception):
    """The source file no longer has the columns the pipeline was written for."""


def parse_extraction_date(first_line):
    match = re.match(r"Estrazione del (\d{4}-\d{2}-\d{2})", first_line.strip())
    if not match:
        raise ValueError(f"unexpected first line: {first_line!r}")
    return datetime.date.fromisoformat(match.group(1))


def read_preamble(path):
    """Returns (extraction date, header columns) from the first two lines of a raw file."""
    with open(path, encoding="utf-8") as f:
        first = f.readline()
        header = f.readline().rstrip("\n")
    return parse_extraction_date(first), header.split("|")


def read_lines(spark, path, expected_columns):
    """Reads a raw file as one row per data line, without splitting it yet.

    Stops if the header changed: loading shifted columns silently is worse than not loading.
    """
    extraction_date, columns = read_preamble(path)
    if columns != expected_columns:
        raise SchemaChangedError(f"{path}: expected {expected_columns}, got {columns}")

    header = "|".join(columns)
    lines = (
        spark.read.text(path)
        .where(~F.col("value").startswith("Estrazione del"))
        .where(F.col("value") != header)
        .where(F.trim("value") != "")
    )
    return extraction_date, lines.select(
        F.col("value").alias("line"),
        F.lit(extraction_date).alias("extraction_date"),
        F.lit(os.path.basename(path)).alias("source_file"),
        F.current_timestamp().alias("ingested_at"),
    )


def download(raw_dir, base_url=BASE_URL):
    """Downloads today's files into raw_dir/<extraction date>/ and returns that date.

    The ministry overwrites the files every day, so this is the only way to build a history.
    """
    contents = {}
    for name in (PRICES_FILE, STATIONS_FILE):
        with urllib.request.urlopen(base_url + name, timeout=60) as response:
            contents[name] = response.read()

    dates = {name: parse_extraction_date(data.split(b"\n", 1)[0].decode("utf-8"))
             for name, data in contents.items()}
    if len(set(dates.values())) != 1:
        raise ValueError(f"files from different days: {dates}")
    extraction_date = dates[PRICES_FILE]

    day_dir = os.path.join(raw_dir, extraction_date.isoformat())
    os.makedirs(day_dir, exist_ok=True)
    for name, data in contents.items():
        # Write then rename, so a crash never leaves half a file behind.
        tmp = os.path.join(day_dir, name + ".tmp")
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, os.path.join(day_dir, name))
    return extraction_date
