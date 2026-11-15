#!/bin/sh
# Downloads today's files and saves them as an archive in the "raw-data" GitHub release.
# Run it once a day after 8:00 (cron), from the repository folder:
#
#     30 10 * * *  cd /path/to/fuel-prices-lakehouse && scripts/archive_day.sh
#
# Requires the GitHub CLI (gh), logged in.
set -eu

python=${PYTHON:-.venv/bin/python}
raw_dir=${RAW_DIR:-data/raw}

day=$("$python" -m fuel_lakehouse download --raw-dir "$raw_dir" | sed -E 's/.*extraction of ([0-9-]+).*/\1/')
archive="${TMPDIR:-/tmp}/$day.tar.gz"
tar -czf "$archive" -C "$raw_dir" "$day"

gh release view raw-data >/dev/null 2>&1 || gh release create raw-data \
    --title "Raw data" --notes "Daily MIMIT exports, one archive per extraction date."
gh release upload raw-data "$archive" --clobber
rm -f "$archive"
echo "archived $day"
