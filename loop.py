import queue
import os
import random
import logging
import os
import concurrent.futures as fut
from pathlib import Path

from backtest.utilities.utils import generate_start_date_in_ms, parse_args, load_credentials, read_universe_list
from trading.broker.broker import SimulatedBroker
from trading.broker.gatekeepers import EnoughCash, MaxPortfolioPercPerInst, NoShort
from trading.portfolio.rebalance import RebalanceLogicalAny, RebalanceYearly, SellLosersMonthly, SellLosersQuarterly, SellWinnersQuarterly
from trading.portfolio.portfolio import FixedTradeValuePortfolio
from backtest.utilities.backtest import backtest
from trading.data.dataHandler import HistoricCSVDataHandler
from trading.strategy.fairprice import FairPriceStrategy, fair_price_ema, minmax_ema 
from trading.strategy.fpmargins import perc_margins
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy
from trading.strategy import ta, broad, statistics
from trading.utilities.enum import OrderPosition
from trading.strategy.personal import profitable

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
    if args.start_ms is not None:
        start_ms = args.start_ms
    elif args.frequency == "day":
        start_ms = generate_start_date_in_ms(2019, 2021)
    else:
        start_ms = generate_start_date_in_ms(2021, 2022)
    end_ms = int(start_ms + random.randint(250, 700) * 8.64e7)  # end anytime between 200 - 800 days later
    print(start_ms, end_ms)
    universe_list = read_universe_list(args.universe)
    symbol_list = set(random.sample(universe_list, min(
        80, len(universe_list))))
    bars = HistoricCSVDataHandler(event_queue, symbol_list, creds,
                                  start_ms=start_ms,
                                  end_ms=end_ms,
                                  frequency_type=args.frequency
                                  )
    period = 20

    if args.frequency == "day":
        strategy = MultipleAllStrategy(bars, event_queue, [  # any of buy and sell
            statistics.ExtremaBounce(
                bars, event_queue, short_period=8, long_period=65, percentile=40),
            MultipleAnyStrategy(bars, event_queue, [
                MultipleAllStrategy(bars, event_queue, [   # buy
                    ta.TALessThan(bars, event_queue, ta.cci,
                                  20, 0, OrderPosition.BUY),
                    # broad.above_functor(bars, event_queue, 'SPY',
                    #     20, args.frequency, OrderPosition.BUY),
                ]),
                MultipleAnyStrategy(bars, event_queue, [   # sell
                    # RelativeExtrema(bars, event_queue, 20, strat_contrarian=False),
                    ta.TAMoreThan(bars, event_queue, ta.rsi,
                                  14, 45, OrderPosition.SELL),
                    ta.TAMoreThan(bars, event_queue, ta.cci,
                                  14, 20, OrderPosition.SELL),
                    ta.TAMin(bars, event_queue, ta.rsi,
                             14, 5, OrderPosition.SELL),
                    broad.below_functor(bars, event_queue, 'SPY',
                                        20, args.frequency, OrderPosition.SELL),
                ], min_matches=2)
            ])
        ])  # StratPreMomentum
        strategy = MultipleAnyStrategy(bars, event_queue, [
            statistics.RelativeExtrema(bars, event_queue, 35, strat_contrarian=True,
                                       percentile=15),  # will keep buying down
            strategy,
        ])
    else:
        strategy = MultipleAllStrategy(bars, event_queue, [  # any of buy and sell
            statistics.ExtremaBounce(
                bars, event_queue, short_period=8, long_period=65, percentile=40),
            MultipleAnyStrategy(bars, event_queue, [
                                MultipleAnyStrategy(bars, event_queue, [   # buy
                                    # MultipleAnyStrategy(bars, event_queue, [
                                    #     fundamental.FundAtLeast(bars, event_queue,
                                    #                             'revenueGrowth', 0.1, order_position=OrderPosition.BUY),
                                    #     fundamental.FundAtLeast(bars, event_queue, 'roe',
                                    #                             0, order_position=OrderPosition.BUY),
                                    # ]),
                                    ta.TALessThan(bars, event_queue, ta.rsi,
                                                  14, 50, OrderPosition.BUY),
                                    ta.TALessThan(bars, event_queue, ta.cci,
                                                  20, 0, OrderPosition.BUY),
                                    broad.above_functor(bars, event_queue, 'SPY', 20, args.frequency, OrderPosition.BUY),
                                ], min_matches=2),
                                MultipleAnyStrategy(bars, event_queue, [   # sell
                                    # RelativeExtrema(bars, event_queue, 20, strat_contrarian=False),
                                    ta.TAMoreThan(bars, event_queue, ta.rsi,
                                                  14, 50, OrderPosition.SELL),
                                    ta.TAMoreThan(bars, event_queue, ta.cci,
                                                  14, 20, OrderPosition.SELL),
                                    ta.TAMin(bars, event_queue, ta.rsi, 14, 5, OrderPosition.SELL),
                                    broad.below_functor(bars, event_queue, 'SPY', 20, args.frequency, OrderPosition.SELL),
                                ], min_matches=2)
                                ])
        ])  # StratPreMomentum

        strategy = profitable.trading_idea_two(bars, event_queue)
        strategy = FairPriceStrategy(bars, event_queue, minmax_ema(period), perc_margins(0.02), int(period * 1.5)) 

    rebalance_strat = RebalanceLogicalAny(bars, event_queue, [
        RebalanceYearly(bars, event_queue),
        SellWinnersQuarterly(bars, event_queue, 0.40),
        SellLosersQuarterly(bars, event_queue, 0.14),
        SellLosersMonthly(bars, event_queue, 0.075),
    ])
    port = FixedTradeValuePortfolio(bars, event_queue, order_queue,
                                    trade_value=700,
                                    max_qty=10,
                                    portfolio_name=(
                                        args.name if args.name != "" else "loop"),
                                    expires=1,
                                    rebalance=rebalance_strat,
                                    initial_capital=INITIAL_CAPITAL
                                    )
    broker = SimulatedBroker(bars, port, event_queue, order_queue, gatekeepers=[
        NoShort(), EnoughCash(),
        # PremiumLimit(150), MaxInstPosition(3), MaxPortfolioPosition(24) # test real scenario since I'm poor
        MaxPortfolioPercPerInst(bars, 0.2)
    ])
    backtest(bars, creds, event_queue, order_queue,
             strategy, port, broker, args.frequency, show_plot=args.num_runs == 1, initial_capital=INITIAL_CAPITAL)


if __name__ == "__main__":
    INITIAL_CAPITAL = 10000
    args = parse_args()
    creds = load_credentials(args.credentials)

    if args.name != "":
        logging.basicConfig(filename=Path(os.environ["WORKSPACE_ROOT"]) /
                            f"Data/data/logging/{args.name}.log", level=logging.INFO, force=True)
    processes = []
    with fut.ProcessPoolExecutor(4) as e:
        for i in range(args.num_runs):
            processes.append(e.submit(main))
    processes = [p.result() for p in processes]
