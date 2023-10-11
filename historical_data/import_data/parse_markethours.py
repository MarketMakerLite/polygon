import pathlib
import logging
from datetime import datetime
import pandas as pd
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.data import InstrumentStatus
from nautilus_trader.model.enums import MarketStatus
from historical_data.import_data.polygon_equity_data import PolygonEquityData


def parse_markethours(markethours_file, instrument):
    """parse market hours file and create instrumentstatus
    nautilus objects

    Args:
        markethours_file (str): csv file containing markethours
        instrument (nautilus_trader.model.instruments.Instrument): nautilus instrument

    Returns:
        list: list of InstrumentStatus nautilus objects
    """
    logging.info(f"reading file : {markethours_file}")
    df_hours = pd.read_csv(markethours_file)
    status = []
    for _, row in df_hours.iterrows():
        if "-" in row["Trading Hours"]:
            times = row["Trading Hours"].split("-")
            start = times[0]
            end = times[1]
            start_unix = dt_to_unix_nanos(datetime.strptime(start, "%Y%m%d:%H%M"))
            end_unix = dt_to_unix_nanos(datetime.strptime(end, "%Y%m%d:%H%M"))
            status.append(
                InstrumentStatus(
                    instrument_id=instrument.id,
                    status=MarketStatus.PRE_OPEN,
                    ts_event=start_unix,
                    ts_init=start_unix,
                )
            )
            status.append(
                InstrumentStatus(
                    instrument_id=instrument.id,
                    status=MarketStatus.CLOSED,
                    ts_event=end_unix,
                    ts_init=end_unix,
                )
            )

        if "-" in row["Liquid Hours"]:
            times = row["Liquid Hours"].split("-")
            start = times[0]
            end = times[1]
            start_unix = dt_to_unix_nanos(datetime.strptime(start, "%Y%m%d:%H%M"))
            end_unix = dt_to_unix_nanos(datetime.strptime(end, "%Y%m%d:%H%M"))
            status.append(
                InstrumentStatus(
                    instrument_id=instrument.id,
                    status=MarketStatus.OPEN,
                    ts_event=start_unix,
                    ts_init=start_unix,
                )
            )
            status.append(
                InstrumentStatus(
                    instrument_id=instrument.id,
                    status=MarketStatus.PRE_CLOSE,
                    ts_event=end_unix,
                    ts_init=end_unix,
                )
            )
    return status


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    poly = PolygonEquityData(None, None, "/workspaces/data")
    instrument = poly.read_catalog_instruments(instrument_ids="QQQ")[0]
    filepath = pathlib.Path(__file__).parent.joinpath("MarketHours.csv")
    status = parse_markethours(filepath, instrument)
    print(status)
