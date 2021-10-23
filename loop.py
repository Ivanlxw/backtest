from backtest.strategy import profitable
from Data.DataWriters.Prices import ABSOLUTE_BT_DATA_DIR
from pathlib import Path
import queue
import os
import random
import logging
import os
from trading.utilities.enum import OrderPosition
import pandas as pd
import concurrent.futures as fut

from backtest.utilities.utils import generate_start_date, parse_args, remove_bs, load_credentials
from backtest.broker import SimulatedBroker
from trading.portfolio.rebalance import RebalanceLogicalAny, RebalanceYearly, SellLosersHalfYearly, RebalanceWeekly
from trading.portfolio.portfolio import PercentagePortFolio
from trading.portfolio.strategy import LongOnly, ProgressiveOrder, SellLowestPerforming
from backtest.utilities.backtest import backtest
from trading.data.dataHandler import HistoricCSVDataHandler
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy, MultipleSendAllStrategy
from trading.strategy import ta, statistics, fundamental

# with open(ABSOLUTE_BT_DATA_DIR / "us_stocks.txt") as fin:
#     SYM_LIST = list(map(remove_bs, fin.readlines()))
SYM_LIST = []
with open(ABSOLUTE_BT_DATA_DIR / "snp500.txt") as fin:
    SYM_LIST += list(set(map(remove_bs, fin.readlines())))

def main():
    event_queue = queue.LifoQueue()
    order_queue = queue.Queue()
    # YYYY-MM-DD
    start_date = generate_start_date()
    while pd.Timestamp(start_date).dayofweek > 4:
        start_date = generate_start_date()
    print(start_date)
    bars = HistoricCSVDataHandler(event_queue,
                                  random.sample(SYM_LIST, 150),
                                #   start_date=start_date,
                                    frequency_type=args.frequency
                                  )

    strat_value = MultipleAllStrategy(bars, event_queue, [
        statistics.ExtremaBounce(
            bars, event_queue, short_period=5, long_period=80, percentile=10),
        # RelativeExtrema(bars, event_queue, long_time=50, percentile=10, strat_contrarian=True),
        ta.VolAboveSMA(bars, event_queue, 10, OrderPosition.BUY),
        ta.TAMax(bars, event_queue, ta.rsi, 14, 7, OrderPosition.BUY),
        MultipleAnyStrategy(bars, event_queue, [
            fundamental.FundAtLeast(
                bars, event_queue, 'revenueGrowth', 0.03, order_position=OrderPosition.BUY),
            fundamental.FundAtLeast(
                bars, event_queue, 'netIncomeGrowth', 0.05, order_position=OrderPosition.BUY),
            fundamental.FundAtLeast(bars, event_queue, 'roe',
                                    0, order_position=OrderPosition.BUY)
        ]),
    ])  # InformValueWithTA

    strategy = MultipleAllStrategy(bars, event_queue, [
        MultipleAnyStrategy(bars, event_queue, [
            fundamental.FundAtLeast(bars, event_queue, 'roe', 0.03, OrderPosition.BUY),
            fundamental.FundAtMost(bars, event_queue, 'roe', 0.05, OrderPosition.SELL),
        ]),
        MultipleAnyStrategy(bars, event_queue, [
            statistics.ExtremaBounce(bars, event_queue, 6, 80, 15),
            ta.MeanReversionTA(bars, event_queue, 20, ta.rsi, sd=2),
        ])
    ])

    
    strategy = MultipleSendAllStrategy(bars, event_queue, [
        # profitable.another_TA(bars, event_queue),
        strategy,
        strat_value,
        profitable.momentum_with_spy(bars, event_queue),    # buy only
        profitable.momentum_vol_with_spy(bars, event_queue),    # buy only
    ])

    rebalance_strat = RebalanceLogicalAny(bars, event_queue, [
        SellLosersHalfYearly(bars, event_queue),
        # RebalanceWeekly(bars, event_queue),
        RebalanceYearly(bars, event_queue)
    ])
    port = PercentagePortFolio(bars, event_queue, order_queue,
                               percentage=0.05,
                               portfolio_name=(
                                   args.name if args.name != "" else "loop"),
                               mode='asset',
                               expires=7,
                               rebalance=rebalance_strat,
                               portfolio_strategy=LongOnly,
                               )
    broker = SimulatedBroker(bars, port, event_queue, order_queue)
    backtest(bars, event_queue, order_queue,
             strategy, port, broker, start_date=start_date, show_plot=args.num_runs == 1)


if __name__ == "__main__":
    args = parse_args()
    load_credentials(args.credentials)

    if args.name != "":
        logging.basicConfig(filename=Path(os.environ["WORKSPACE_ROOT"]) /
                            f"Data/logging/{args.name}.log", level=logging.INFO, force=True)
    processes = []
    with fut.ProcessPoolExecutor(4) as e:
        for i in range(args.num_runs):
            processes.append(e.submit(main))
    processes = [p.result() for p in processes]
