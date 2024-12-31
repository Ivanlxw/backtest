import importlib
import json
import os
import logging
import os
import concurrent.futures as fut
from pathlib import Path

import matplotlib.pyplot as plt

from backtest.utilities.backtest import Backtest
from backtest.utilities.utils import generate_start_date_in_ms, parse_args, load_credentials, read_universe_list
from trading.broker.broker import SimulatedBroker
from trading.data.dataHandler import DBDataHandler


def main(args):
    creds = args.creds
    with open(args.data_config_fp, 'r') as fin:
        data_config = json.load(fin)

    symbol_list = [c["symbol"] for c in data_config["contracts"]]
    bars = DBDataHandler(symbol_list, data_config, creds)
    setattr(args, "data_provider", bars)
    broker = SimulatedBroker(bars, args.portfolio, gatekeepers=args.gk)
    setattr(args, "broker", broker)

    bt = Backtest(args)
    bt.run(live=False)

    # args.start_ms = start_ms
    # args.end_ms = end_ms

    # plot_index_benchmark(args, ["SPY"], "BuyAndHoldIndex")
    # if args.inst_type == "equity":
    #     plot_index_benchmark(args, symbol_list, "BuyAndHoldStrategy")

    # if args.name and args.save_portfolio:
    #     args.port.write_curr_holdings()
    #     args.port.write_all_holdings()

    if bt.show_plot:
        plt.legend()
        plt.show()


if __name__ == "__main__":
    args = parse_args()
    model_args = importlib.import_module(f"backtest.config.{args.config_name}").get_config()
    model_args["creds"] = load_credentials(model_args["credentials_fp"])
    for k, v in model_args.items():
        setattr(args, k, v)

    if args.name != "":
        logging.basicConfig(
            filename=Path(os.environ["DATA_DIR"]) / f"logging/{args.name}.log", level=logging.INFO, force=True
        )
    with fut.ProcessPoolExecutor(4) as e:
        processes = [e.submit(main, args) for _ in range(args.num_runs)]
        processes = [p.result() for p in processes]
