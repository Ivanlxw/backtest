import importlib
import json
import os
import logging
import os
import concurrent.futures as fut
from pathlib import Path

import matplotlib.pyplot as plt

from backtest.utilities.backtest import Backtest
from backtest.utilities.utils import parse_args, load_credentials
from trading.broker.broker import SimulatedBroker
from trading.data.dataHandler import DBDataHandler
from trading.portfolio.portfolio import PercentagePortFolio
from trading.portfolio.rebalance import NoRebalance
from trading.strategy.basic import BuyAndHoldStrategy
from trading.utilities.enum import OrderType




def run_baseline(symbol_list, data_config, creds, portfolio_name):
    bars = DBDataHandler(symbol_list, data_config, creds)
    strategy = BuyAndHoldStrategy()
    portfolio = PercentagePortFolio(
        1 / len(symbol_list) - 0.001,
        rebalance=NoRebalance(),
        portfolio_name=portfolio_name,
        initial_capital=args.initial_capital,
        order_type=OrderType.LIMIT,
        load_portfolio_details=args.load_portfolio,
    )
    setattr(args, "data_provider", bars)
    setattr(args, "strategy", strategy)
    setattr(args, "portfolio", portfolio)
    args.portfolio.Initialize(symbol_list, bars.option_metadata_info)
    broker = SimulatedBroker(bars, args.portfolio, gatekeepers=args.gk)
    setattr(args, "broker", broker)
    bt = Backtest(args)
    bt.run(live=False)
    return bt

def main(args):
    creds = args.creds
    with open(args.data_config_fp, 'r') as fin:
        data_config = json.load(fin)

    symbol_list = [c["symbol"] for c in data_config["contracts"]]
    bars = DBDataHandler(symbol_list, data_config, creds)
    setattr(args, "data_provider", bars)
    broker = SimulatedBroker(bars, args.portfolio, gatekeepers=args.gk)
    setattr(args, "broker", broker)
    args.portfolio.Initialize(symbol_list, bars.option_metadata_info)

    bt = Backtest(args)
    bt.run(live=False)

    # benchmark - buynhold
    bt2 = run_baseline(symbol_list, data_config, creds, "BuynHold")
    bt3 = run_baseline(["SPY", "QQQ"], data_config, creds, "IndexBM")

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
