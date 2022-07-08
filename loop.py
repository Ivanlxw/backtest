import queue
import os
import random
import logging
import os
import pandas as pd
import concurrent.futures as fut
from pathlib import Path

from backtest.strategy import profitable
from backtest.utilities.utils import generate_start_date_after_2015, parse_args, load_credentials
from trading.broker.broker import SimulatedBroker
from trading.broker.gatekeepers import EnoughCash, MaxPortfolioPercPerInst, NoShort, PremiumLimit, MaxPortfolioPosition, MaxInstPosition
from trading.portfolio.rebalance import RebalanceLogicalAny, RebalanceYearly, SellLosersMonthly, SellWinnersQuarterly
from trading.portfolio.portfolio import FixedTradeValuePortfolio
from backtest.utilities.backtest import backtest
from trading.data.dataHandler import HistoricCSVDataHandler
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy
from trading.strategy import ta, broad, fundamental, statistics
from trading.utilities.enum import OrderPosition
from trading.utilities.utils import get_etf_list, get_trading_universe

''' tRial strategies
def extrema_bounce_ta(bars, event_queue, extrema_period=80, ta_func: Callable = ta.sma, ta_period=20, exit: bool = True):
    return MultipleAllStrategy(bars, event_queue, [
        statistics.ExtremaBounce(bars, event_queue, 6, extrema_period, 15),
        ta.MeanReversionTA(bars, event_queue, ta_period,
                           ta_func, sd=2, exit=exit),
    ], "BounceTA: " + f"extrema_period={extrema_period}")


def extrema_bounce_rsi(bars, event_queue, extrema_period=80, ta_period=20, exit: bool = True):
    return extrema_bounce_ta(bars, event_queue, extrema_period, ta.rsi, ta_period, exit)

'''


def main():
    event_queue = queue.LifoQueue()
    order_queue = queue.Queue()
    # YYYY-MM-DD
    start_date = generate_start_date_after_2015()
    while pd.Timestamp(start_date).dayofweek > 4:
        start_date = generate_start_date_after_2015()
    print(start_date)
    universe_list = get_trading_universe(args.universe)
    symbol_list = set(random.sample(universe_list, min(
        80, len(universe_list))))
    bars = HistoricCSVDataHandler(event_queue, symbol_list, creds,
                                  start_date=start_date,
                                  frequency_type=args.frequency
                                  )

    if args.frequency == "daily":
        strat_pre_momentum = MultipleAllStrategy(bars, event_queue, [  # any of buy and sell
            statistics.ExtremaBounce(
                bars, event_queue, short_period=8, long_period=80, percentile=40),
            MultipleAnyStrategy(bars, event_queue, [
                MultipleAllStrategy(bars, event_queue, [   # buy
                    MultipleAnyStrategy(bars, event_queue, [
                        fundamental.FundAtLeast(bars, event_queue,
                                                'revenueGrowth', 0.1, order_position=OrderPosition.BUY),
                        fundamental.FundAtLeast(bars, event_queue, 'roe',
                                                0, order_position=OrderPosition.BUY),
                    ]),
                    ta.TALessThan(bars, event_queue, ta.cci,
                                20, 0, OrderPosition.BUY),
                ]),
                MultipleAnyStrategy(bars, event_queue, [   # sell
                    # RelativeExtrema(bars, event_queue, 20, strat_contrarian=False),
                    ta.TAMoreThan(bars, event_queue, ta.rsi,
                                14, 50, OrderPosition.SELL),
                    ta.TAMoreThan(bars, event_queue, ta.cci,
                                14, 20, OrderPosition.SELL),
                    ta.TAMin(bars, event_queue, ta.rsi, 14, 5, OrderPosition.SELL),
                    broad.below_functor(bars, event_queue, 'SPY',
                                        20, OrderPosition.SELL),
                ], min_matches=2)
            ])
        ])  # StratPreMomentum
        # strategy = MultipleAnyStrategy(bars, event_queue, [
        #     strat_pre_momentum,
        #     profitable.comprehensive_with_value_bounce(bars, event_queue)
        # ])
        strategy = profitable.strict_comprehensive_longshort(bars, event_queue, ma_value=22, trending_score=-0.05)
    else:
        strategy = MultipleAnyStrategy(bars, event_queue, [
            profitable.strict_comprehensive_longshort(
                bars, event_queue, trending_score=-0.1),
            ta.MABounce(bars, event_queue, ta.ema, 25),
            ta.MABounce(bars, event_queue, ta.ema, 50),
            profitable.value_extremaTA(bars, event_queue),
            profitable.momentum_with_TACross(bars, event_queue),
        ])
    rebalance_strat = RebalanceLogicalAny(bars, event_queue, [
        RebalanceYearly(bars, event_queue),
        SellWinnersQuarterly(bars, event_queue, 0.35),
        SellLosersMonthly(bars, event_queue, 0.1)
    ])
    port = FixedTradeValuePortfolio(bars, event_queue, order_queue,
                                    trade_value=250,
                                    portfolio_name=(
                                        args.name if args.name != "" else "loop"),
                                    expires=1,
                                    rebalance=rebalance_strat,
                                    initial_capital=INITIAL_CAPITAL
                                    )
    broker = SimulatedBroker(bars, port, event_queue, order_queue, gatekeepers=[
        NoShort(), EnoughCash(), 
        PremiumLimit(150), MaxInstPosition(3), MaxPortfolioPosition(24) # test real scenario since I'm poor
        # MaxPortfolioPercPerInst(bars, 0.4) 
    ])
    backtest(bars, creds, event_queue, order_queue,
             strategy, port, broker, start_date=start_date, show_plot=args.num_runs == 1, initial_capital=INITIAL_CAPITAL)


if __name__ == "__main__":
    INITIAL_CAPITAL = 2500
    args = parse_args()
    creds = load_credentials(args.credentials)

    if args.name != "":
        logging.basicConfig(filename=Path(os.environ["WORKSPACE_ROOT"]) /
                            f"Data/logging/{args.name}.log", level=logging.INFO, force=True)
    processes = []
    with fut.ProcessPoolExecutor(4) as e:
        for i in range(args.num_runs):
            processes.append(e.submit(main))
    processes = [p.result() for p in processes]
