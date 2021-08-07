import json
import queue
import os
import random
import logging
import os
from trading.utilities.enum import OrderPosition
import pandas as pd
from pathlib import Path
import concurrent.futures as fut

from backtest.utilities.utils import MODELINFO_DIR, generate_start_date, parse_args, remove_bs, load_credentials
from backtest.broker import SimulatedBroker
from backtest.portfolio.rebalance import NoRebalance, RebalanceBiennial, RebalanceHalfYearly, RebalanceYearly
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.portfolio.strategy import DefaultOrder, LongOnly, ProgressiveOrder, SellLowestPerforming
from backtest.utilities.backtest import backtest
from trading.data.dataHandler import HistoricCSVDataHandler
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy
from trading.strategy.statistics import ExtremaBounce, LongTermCorrTrend, RelativeExtrema
from trading.strategy import ta, broad, fundamental

ABSOLUTE_FILEDIR = Path(os.path.dirname(os.path.abspath(__file__)))
with open(f"{os.path.abspath(os.path.dirname(__file__))}/Data/snp500.txt", 'r') as fin:
    stock_list = fin.readlines()
stock_list = list(map(remove_bs, stock_list))
symbol_list = stock_list

def main():
    event_queue = queue.LifoQueue()
    order_queue = queue.Queue()
    # YYYY-MM-DD
    start_date = generate_start_date()
    while pd.Timestamp(start_date).dayofweek > 4:
        start_date = generate_start_date()
    print(start_date)
    end_date = "2020-01-30"


    bars = HistoricCSVDataHandler(event_queue,
                                random.sample(symbol_list, 250),
                                start_date=start_date,
                                end_date=end_date
                                )

    strategy = MultipleAllStrategy(bars, event_queue, [
        ExtremaBounce(bars, event_queue, short_period=6,long_period=80, percentile=10),
        broad.below_sma(bars, event_queue, "SPY", 20, OrderPosition.BUY),
        ta.TAMax(bars, event_queue, ta.rsi, 14, 7, OrderPosition.BUY),
        fundamental.FundAtLeast(bars, event_queue, 'revenueGrowth', 0.05, order_position=OrderPosition.BUY),
    ])

    if args.name != "":
        with open(MODELINFO_DIR / f'{args.name}.json', 'w') as fout:
            fout.write(json.dumps(strategy.describe()))

    port = PercentagePortFolio(bars, event_queue, order_queue,
                            percentage=0.10,
                            portfolio_name=(
                                args.name if args.name != "" else "loop"),
                            mode='asset',
                            expires=7,
                            rebalance=RebalanceYearly,
                            portfolio_strategy=LongOnly,
                            )
    broker = SimulatedBroker(bars, port, event_queue, order_queue)
    backtest(bars, event_queue, order_queue,
            strategy, port, broker, start_date=start_date, show_plot=args.num_runs == 1)

if __name__ == "__main__":
    args = parse_args()
    load_credentials(args.credentials)

    if args.name != "":
        logging.basicConfig(filename=ABSOLUTE_FILEDIR /
                            f"Data/logging/{args.name}.log", level=logging.INFO)
    processes = []
    with fut.ProcessPoolExecutor(4) as e:
        for i in range(args.num_runs):
            processes.append(e.submit(main))
    processes = [p.result() for p in processes]