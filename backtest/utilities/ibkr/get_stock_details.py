import argparse
import os
import threading
import time

import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text

from ibapi.client import *
from ibapi.wrapper import *
from ibapi.contract import Contract
from ibapi.order import *

from backtest.utilities.utils import read_universe
from backtest.utilities.ibkr._base import IBClient


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a space-delimited list of strings.")
    parser.add_argument(
        '-s', '--symbol',
        nargs='+',
        required=False,
        default=None,
        help='A space-delimited list of symbols',
    )
    parser.add_argument(
        '-f', '--filepath',
        required=False,
        default=None,
        help="A txt file containing a list of symbols to query"
    )
    parser.add_argument(
        '--save-db', action='store_true', default=False,
        help="save to db or print out info"
    )
    args = parser.parse_args()

    eng = create_engine(os.environ["DB_URL"])
    saved_symbols = pd.read_sql('''
        SELECT distinct symbol FROM ibkr.symbol_info
    ''', eng).squeeze().to_list()

    c = IBClient(eng, args.save_db)
    req_id = 12111
    if args.symbol is not None:
        sym_to_query = [s for s in args.symbol if s.upper() not in saved_symbols]
        for sym in sym_to_query:
            contract = Contract()
            contract.symbol = sym
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"
            c.reqContractDetails(req_id, contract)
            req_id += 11
    elif args.filepath is not None:
        universe = read_universe(args.filepath)
        universe = [s for s in universe if s not in saved_symbols]
        for sym in universe:
            contract = Contract()
            contract.symbol = sym
            contract.secType = "STK"
            contract.exchange = "SMART"
            contract.currency = "USD"
            c.reqContractDetails(req_id, contract)
            req_id += 11
    else:
        raise Exception("symbol or filepath has to be supplied as argument")