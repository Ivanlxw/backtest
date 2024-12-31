import importlib
import json
import os
import logging
import os
import concurrent.futures as fut
from pathlib import Path

import matplotlib.pyplot as plt

from backtest.utilities.backtest import Backtest
from backtest.utilities.utils import parse_args, load_credentials, read_universe_list
from trading.broker.broker import IBBroker
from trading.portfolio.portfolio import PercentagePortFolio
from trading.data.dataHandler import StreamingDataHandler


def main(args):
    creds = args.creds
    with open(args.data_config_fp, 'r') as fin:
        data_config = json.load(fin)

    symbol_list = [c["symbol"] for c in data_config["contracts"]]
    bars = StreamingDataHandler(symbol_list, creds)
    setattr(args, "data_provider", bars)

    args.portfolio.Initialize(
        bars.symbol_list,
        # bars.start_ms,
        bars.option_metadata_info,
    )
    broker = IBBroker(args.portfolio, args.gk, args.is_live_acct)
    broker.reqAccountSummary(9001, "All", 'NetLiquidation,TotalCashValue,AvailableFunds,ExcessLiquidity')
    setattr(args, "broker", broker)

    bt = Backtest(args)
    bt.run(live=False)
    
    broker.disconnect()
    broker.api_thread.join()

    if bt.show_plot:
        plt.legend()
        plt.show()


if __name__ == "__main__":
    args = parse_args()
    model_args = importlib.import_module(f"backtest.config.{args.config_name}").get_config()
    model_args["creds"] = load_credentials(model_args["credentials_fp"])
    for k, v in model_args.items():
        setattr(args, k, v)
    # hacky. TODO: better way to put broker acct info in program
    for k, v in model_args["creds"].items():
        os.environ[k] = v

    if args.name != "":
        logging.basicConfig(
            filename=Path(os.environ["DATA_DIR"]) / f"logging/{args.name}.log", level=logging.INFO, force=True
        )
    with fut.ProcessPoolExecutor(4) as e:
        processes = [e.submit(main, args) for _ in range(args.num_runs)]
        processes = [p.result() for p in processes]
