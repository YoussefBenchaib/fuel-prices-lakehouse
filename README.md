# Fuel prices lakehouse

A batch pipeline in PySpark that turns Italy's daily fuel price open data into clean, partitioned
Parquet tables, with data quality checks, a quarantine for rejected rows, and reconciliation of every
row from input to output. On top of those tables, a dbt project builds the analytics models in SQL
and tests them.

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
               │                                       and self/served, without stale prices
               │
          _reports/2026-10-20.json                     row counts and rejection reasons
               │
               ▼   dbt (DuckDB reads the Parquet files)
          stg_prices ─► dim_stations
                     ─► fct_province_daily_prices      checked against Spark's gold table
                     ─► fct_station_price_position     gap to the province average, rank
                     ─► fct_brand_daily_gap            how each brand prices against its market
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

## dbt models

The Spark pipeline answers "is this row valid?". The dbt project in `dbt/` answers business questions
on the valid rows, in SQL. It runs on DuckDB, which reads the lake's Parquet files directly, so it
needs no server or account.

| Model | One row per | What it's for |
|---|---|---|
| `stg_prices` | day, station, fuel, self/served | clean prices with the names used downstream |
| `dim_stations` | station | brand, municipality, province, coordinates |
| `fct_province_daily_prices` | day, province, fuel, self/served | average, minimum and maximum price |
| `fct_station_price_position` | day, station, fuel, self/served | gap to the province average and rank from cheapest |
| `fct_brand_daily_gap` | day, brand, fuel, self/served | how each brand prices against its local market |

Self-service petrol on 2026-11-14, brands with at least 300 stations (`fct_brand_daily_gap`):

```
brand            stations   avg_price   avg_gap_to_province
Agip Eni             3841       1.946                -0.015
Q8                   2652       1.950                -0.011
Tamoil               1449       1.962                +0.002
Api-Ip               3446       1.964                +0.003
Esso                 1932       1.966                +0.004
Pompe Bianche        2522       1.982                +0.017
```

`dbt build` runs 22 data tests: keys are unique, required columns are not null, prices stay in range,
every station in a fact table exists in `dim_stations`, and gaps to the average cancel out within each
province.

One test compares `fct_province_daily_prices` with the gold table computed in Spark, row by row. The
first time it ran, it failed on one row out of 6,862: four prices adding up to 8.030, so an average of
exactly 2.0075. Spark averages decimals and rounds to 2.008; DuckDB's `avg()` returns a floating-point
number just below 2.0075 and rounds to 2.007. The dbt models now compute averages in whole
milli-euros (`macros/exact_avg.sql`), and the two engines agree on every row.

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

cd dbt && ../.venv/bin/dbt build --profiles-dir .      # models and data tests, in dbt/target/fuel_prices.duckdb
```

`sample/raw/` contains a small extract of a real day (77 stations, with operators and addresses
replaced by placeholders), enough to try everything without downloading:

```sh
.venv/bin/python -m fuel_lakehouse process --raw-dir sample/raw
```

To build a history, run `download` once a day after 8:00: the ministry keeps only the latest day online.

## Tests

```sh
.venv/bin/pytest
```

The tests build small raw files in the same format as the ministry's and check each quality rule,
the stale flag, reconciliation, re-runs, header changes and partition pruning. On every push, GitHub
Actions runs them, then runs the pipeline and `dbt build` on the sample day.

## Next steps

- Store silver as an Apache Iceberg table, for atomic commits, time travel and schema evolution.
- Day-over-day price changes per station, once more days are collected.
- Run the same dbt models on a cloud warehouse (Snowflake or BigQuery) by adding a second target.
