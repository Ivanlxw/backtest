import argparse
import os
from pathlib import Path
from sqlalchemy import create_engine

import polars as pl

from backtest.utilities.utils import load_credentials, read_universe_list
from Data.source.polygon import Polygon

COLS_TO_SAVE = [
    "ticker",
    "name",
    "market",
    "locale",
    "primary_exchange",
    "type",
    "active",
    "currency_name",
    "cik",
    "composite_figi",
    "share_class_figi",
    "market_cap",
    "phone_number",
    "description",
    "sic_code",
    "sic_description",
    "ticker_root",
    "total_employees",
    "list_date",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Get ticker csv data via API calls to either AlphaVantage or Tiingo."
    )
    parser.add_argument(
        "-c",
        "--credentials",
        required=True,
        type=str,
        help="filepath to credentials.json",
    )
    parser.add_argument(
        "--universe",
        type=Path,
        required=True,
        help="File path to trading universe",
        nargs="+",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    load_credentials(args.credentials, into_env=True)
    universe_list = read_universe_list(args.universe)
    p = Polygon("equity")
    stock_details = []
    for s in universe_list:
        stock_details.append(p.get_stock_details(s))
    stock_details_df = pl.DataFrame(stock_details)[COLS_TO_SAVE]
    stock_details_df.with_columns(
        [
            pl.col("sic_code").cast(pl.Int64),
            pl.col("list_date").str.to_datetime("%Y-%m-%d"),
        ]
    )

    db_url = os.environ["DB_URL"]
    conn = create_engine(db_url)
    connection = conn.connect()
    existing_df = pl.read_database("SELECT * from backtest.equity_metadata", connection)
    stock_details_df = pl.concat([stock_details_df, existing_df]).unique(keep="first")

    stock_details_df.write_database(
        "backtest.equity_metadata", db_url, if_exists="append"
    )
