# Fuel prices lakehouse

A batch pipeline in PySpark that turns Italy's daily fuel price open data into clean, partitioned
Parquet tables, with data quality checks, a quarantine for rejected rows, and reconciliation of every
row from input to output.

It uses the same government data as my [fuel price monitor](https://github.com/YoussefBenchaib/fuel-price-monitor),
but instead of a few stations every 15 minutes it loads the full national export: about 92,700 prices
from 23,900 stations every day.

## The data

Every morning the Ministry of Enterprises (MIMIT) publishes two files, overwriting the previous day's:

- `prezzo_alle_8.csv`: prices in force at 8:00 the day before, one row per station, fuel and
  self/served mode
- `anagrafica_impianti_attivi.csv`: active stations, with brand, municipality, province and coordinates

They are `|`-separated, with an extra first line (`Estrazione del 2026-10-20`) before the header.
Since only the latest day is online, the history is built by downloading one day at a time.

## How it works

```
download ─► data/raw/2026-10-20/*.csv
               │
               ▼   check the header, keep each line as it is
          bronze/prices, bronze/stations              (Parquet, partitioned by extraction_date)
               │
               ▼   parse, type, check quality ──────► quarantine/prices, quarantine/stations
          silver/prices                                (rejected lines + reason)
               │   clean prices joined with stations
               ▼
          gold/province_prices                         average / min / max per province, fuel
                                                       and self/served, without stale prices
          _reports/2026-10-20.json                     row counts and rejection reasons
```

Result of the run on the 2026-10-20 extraction:

```json
"stations": { "bronze": 23934, "valid": 23933, "rejected": 1,  "reasons": { "malformed_row": 1 } },
"prices":   { "bronze": 92717, "valid": 92703, "rejected": 14,
              "reasons": { "price_out_of_range": 12, "unknown_station": 2 }, "stale": 2001 }
```

## What the real data looks like, and how it's handled

- **The separator appears inside the data.** 113 station names contain `|`
  (`STOIL SIMPLE | gestori.prezzibenzina.it`), which shifts every following column. When there is
  exactly one extra field, it can only belong to the name, so the fields before it are read from the
  left and the ones after it from the right. One station has `|` in two different fields: that can't
  be fixed without guessing, so the row goes to quarantine, and its two prices are rejected as
  `unknown_station`.
- **Impossible prices.** Prices like €0.100 or €8.888 per litre are rejected as `price_out_of_range`
  (accepted range €0.50–4.00).
- **Old prices.** Stations report a price only when it changes, so some rows date back to 2013. They
  are kept in silver with `is_stale = true` (not updated for more than 7 days) and left out of the
  gold averages: an old price is real data, but it shouldn't move today's average.
- **Other checks:** malformed rows, non-numeric ids, invalid `isSelf`, unparseable timestamps,
  timestamps after the extraction day, duplicate keys.

## Design decisions

- **Bronze keeps the raw line.** Parsing rules can change; if they do, everything can be reprocessed
  from bronze without downloading again (the source only keeps one day).
- **Nothing disappears silently.** Every input line ends up either in silver or in quarantine with a
  reason, and the run fails if `bronze ≠ valid + rejected`.
- **A changed header stops the run.** If the ministry adds or renames a column, loading shifted
  columns would be worse than not loading, so the pipeline raises before writing anything.
- **Exact money.** Prices are `DECIMAL(6,3)`, not floating point, so 2.349 stays exactly 2.349.
- **Partitioned by day only.** Queries on one day read only that folder (partition pruning).
  Partitioning by fuel as well would create around 40 tiny files per day for ~100k rows: the
  "small files" problem costs more than it saves at this size.
- **Re-running is safe.** Writes use dynamic partition overwrite: processing a day again replaces
  that day and leaves the others alone.
- **Testable transformations.** Parsing, quality checks and aggregations are pure
  DataFrame-to-DataFrame functions (`quality.py`, `transform.py`); files are read and written only in
  `raw.py` and `pipeline.py`.

## Running it

Requires Python 3.9+ and Java 17 (on macOS: `brew install openjdk@17` and
`export JAVA_HOME=/opt/homebrew/opt/openjdk@17`).

```sh
python -m venv .venv && .venv/bin/pip install -r requirements.txt

.venv/bin/python -m fuel_lakehouse download            # saves today's files in data/raw/<date>/
.venv/bin/python -m fuel_lakehouse process             # loads every day in data/raw into data/lake
.venv/bin/python -m fuel_lakehouse process --date 2026-10-20
```

To build a history, schedule `download` once a day (cron or Airflow) after 8:00.

Querying the result with Spark SQL:

```python
from fuel_lakehouse.spark import get_spark

spark = get_spark()
spark.read.parquet("data/lake/gold/province_prices").createOrReplaceTempView("province_prices")
spark.sql("""
    SELECT fuel, is_self, stations, avg_price, min_price, max_price
    FROM province_prices
    WHERE extraction_date = DATE'2026-10-20' AND province = 'MI' AND fuel IN ('Benzina', 'Gasolio')
""").show()
```

```
+-------+-------+--------+---------+---------+---------+
|   fuel|is_self|stations|avg_price|min_price|max_price|
+-------+-------+--------+---------+---------+---------+
|Benzina|  false|     353|    2.155|    1.979|    2.519|
|Benzina|   true|     646|    2.006|    1.839|    2.399|
|Gasolio|  false|     345|    2.389|    2.129|    2.719|
|Gasolio|   true|     637|    2.242|    1.949|    2.592|
+-------+-------+--------+---------+---------+---------+
```

## Tests

```sh
.venv/bin/pytest
```

The tests build small raw files in the same format as the ministry's and check each quality rule,
the stale flag, reconciliation, re-runs, header changes and partition pruning. They run on every push
with GitHub Actions.

## Next steps

- Store silver as an Apache Iceberg table, for atomic commits, time travel and schema evolution.
- Orchestrate download and processing with Airflow.
