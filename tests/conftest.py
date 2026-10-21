import os

import pytest

from fuel_lakehouse.raw import PRICES_FILE, STATIONS_FILE
from fuel_lakehouse.spark import get_spark

PRICES_HEADER = "idImpianto|descCarburante|prezzo|isSelf|dtComu"
STATIONS_HEADER = ("idImpianto|Gestore|Bandiera|Tipo Impianto|Nome Impianto|Indirizzo|"
                   "Comune|Provincia|Latitudine|Longitudine")


@pytest.fixture(scope="session")
def spark():
    session = get_spark("fuel-prices-lakehouse-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def station(station_id, province="MI", name="STATION"):
    return f"{station_id}|OPERATOR SRL|Brand|Stradale|{name}|VIA ROMA 1|MILANO|{province}|45.46|9.19"


def price(station_id, fuel="Benzina", value="2.000", is_self="1",
          reported="19/10/2026 08:00:00"):
    return f"{station_id}|{fuel}|{value}|{is_self}|{reported}"


@pytest.fixture
def write_day(tmp_path):
    """Writes a raw day in the same format as the ministry's files and returns its folder."""
    def write(prices, stations, date="2026-10-20", prices_header=PRICES_HEADER):
        day_dir = tmp_path / "raw" / date
        day_dir.mkdir(parents=True)
        (day_dir / PRICES_FILE).write_text(
            "\n".join([f"Estrazione del {date}", prices_header, *prices]) + "\n", encoding="utf-8")
        (day_dir / STATIONS_FILE).write_text(
            "\n".join([f"Estrazione del {date}", STATIONS_HEADER, *stations]) + "\n",
            encoding="utf-8")
        return str(day_dir)
    return write


@pytest.fixture
def lake_dir(tmp_path):
    return os.path.join(tmp_path, "lake")
