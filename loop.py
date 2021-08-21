from Data.DataWriter import ABSOLUTE_BT_DATA_DIR
from pathlib import Path
import talib
import json
import queue
import os
import random
import logging
import os
from trading.utilities.enum import OrderPosition
import pandas as pd
import concurrent.futures as fut

from backtest.utilities.utils import MODELINFO_DIR, generate_start_date, parse_args, remove_bs, load_credentials
from backtest.broker import SimulatedBroker
from trading.portfolio.rebalance import RebalanceHalfYearly, RebalanceYearly
from trading.portfolio.portfolio import PercentagePortFolio
from trading.portfolio.strategy import LongOnly
from backtest.utilities.backtest import backtest
from trading.data.dataHandler import HistoricCSVDataHandler
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy
from trading.strategy import ta, broad, statistics
from trading.strategy.statmodels import features, models, targets
from sklearn.linear_model import Lasso, Ridge

with open(f"{os.path.abspath(os.path.dirname(__file__))}/Data/us_stocks.txt", 'r') as fin:
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

    strat_momentum = MultipleAnyStrategy(bars, event_queue, [  # any of buy and sell
        MultipleAllStrategy(bars, event_queue, [   # buy
            ta.TALessThan(bars, event_queue, ta.rsi,
                            14, 45, OrderPosition.BUY),
            broad.above_sma(bars, event_queue, 'SPY',
                            25, OrderPosition.BUY),
            ta.TALessThan(bars, event_queue, ta.cci,
                        20, -70, OrderPosition.BUY),
            ta.TAMax(bars, event_queue, ta.rsi, 14, 5, OrderPosition.BUY)
        ]),
        MultipleAllStrategy(bars, event_queue, [   # sell
            # RelativeExtrema(bars, event_queue, 20, strat_contrarian=False),
            ta.TAMoreThan(bars, event_queue, ta.rsi,
                            14, 50, OrderPosition.SELL),
            ta.TAMoreThan(bars, event_queue, ta.cci,
                            14, 70, OrderPosition.SELL),
            ta.TAMin(bars, event_queue, ta.rsi, 14, 5, OrderPosition.SELL),
            broad.below_sma(bars, event_queue, 'SPY',
                            20, OrderPosition.SELL),
        ])
    ])

    strategy = MultipleAllStrategy(bars, event_queue, [
        statistics.ExtremaBounce(bars, event_queue, 7, 100, percentile=20),
        ta.MeanReversionTA(bars, event_queue, 20, talib.SMA, exit=True),
    ])

    strategy = MultipleAnyStrategy(bars, event_queue, [
        strat_momentum, strategy
    ])
    feat = [
        features.RSI(14),
        features.RelativePercentile(50),
        features.QuarterlyFundamental(bars, 'roe'),
        features.QuarterlyFundamental(bars, 'pbRatio'),
        features.QuarterlyFundamental(bars, 'grossProfitGrowth')
    ]
    target = targets.EMAClosePctChange(30)

    strategy = models.SkLearnRegModelNormalized(
        bars, event_queue, Ridge, feat, target, RebalanceHalfYearly,
        order_val=0.08,
        n_history=60,
        params={
            "fit_intercept": False,
            "alpha": 0.5,
        },
        live=args.live
    )
    strategy = MultipleAllStrategy(bars, event_queue, [
        strategy,
        MultipleAnyStrategy(bars, event_queue, [
            MultipleAllStrategy(bars, event_queue, [
                ta.TALessThan(bars, event_queue, ta.rsi,
                            14, 40, OrderPosition.BUY),
                # ta.TAMax(bars, event_queue, ta.rsi, 14, 5, OrderPosition.BUY),
            ]),
            MultipleAllStrategy(bars, event_queue, [
                ta.TAMoreThan(bars, event_queue, ta.rsi,
                            14, 60, OrderPosition.SELL),
                # ta.TAMin(bars, event_queue, ta.rsi, 14, 6, OrderPosition.SELL),
            ]),
        ])
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
    curr_holdings_fp = ABSOLUTE_BT_DATA_DIR / f"portfolio/{port.name}.json"
    port.write_all_holdings(curr_holdings_fp)

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
