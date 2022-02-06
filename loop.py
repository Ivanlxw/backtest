import queue
import os
import random
import logging
import os
import pandas as pd
import concurrent.futures as fut
from pathlib import Path

from backtest.strategy import profitable
from backtest.utilities.utils import generate_start_date, parse_args, remove_bs, load_credentials
from trading.broker.broker import SimulatedBroker
from trading.broker.gatekeepers import EnoughCash, MaxPortfolioValuePerInst, NoShort, PremiumLimit
from trading.portfolio.rebalance import RebalanceLogicalAny, RebalanceYearly, SellWinnersQuarterly
from trading.portfolio.portfolio import PercentagePortFolio, Portfolio
from backtest.utilities.backtest import backtest
from trading.data.dataHandler import HistoricCSVDataHandler
from trading.strategy.basic import OneSidedOrderOnly
from trading.strategy.multiple import MultipleAllStrategy, MultipleAnyStrategy, MultipleSendAllStrategy
from trading.strategy import ta, statistics, fundamental
from trading.strategy.complex.complex_high_beta import ComplexHighBeta
from trading.utilities.enum import OrderPosition
from trading.utilities.utils import DOW_LIST, SNP_LIST, NASDAQ_LIST, ETF_LIST


def main():
    event_queue = queue.LifoQueue()
    order_queue = queue.Queue()
    # YYYY-MM-DD
    start_date = generate_start_date()
    while pd.Timestamp(start_date).dayofweek > 4:
        start_date = generate_start_date()
    print(start_date)
    bars = HistoricCSVDataHandler(event_queue,
                                  random.sample(DOW_LIST + SNP_LIST + NASDAQ_LIST, 200) + ETF_LIST,
                                  start_date=start_date,
                                  frequency_type=args.frequency
                                  )

    strat_value = MultipleAllStrategy(bars, event_queue, [
        statistics.ExtremaBounce(
            bars, event_queue, short_period=7, long_period=100, percentile=10),
        statistics.EitherSide(bars, event_queue, 100, 20),
        MultipleAnyStrategy(bars, event_queue, [
            MultipleAllStrategy(bars, event_queue, [
                fundamental.FundAtLeast(bars, event_queue, 'roic',
                                        0, order_position=OrderPosition.BUY),
                fundamental.FundAtLeast(
                    bars, event_queue, 'operatingIncomeGrowth', 0.1, order_position=OrderPosition.BUY),
                fundamental.FundAtLeast(bars, event_queue, 'returnOnEquity',
                                        0.03, order_position=OrderPosition.BUY),
                ta.VolAboveSMA(bars, event_queue, 10, OrderPosition.BUY),
                ta.TAMax(bars, event_queue, ta.rsi, 14, 7, OrderPosition.BUY),
            ]),  # buy
            MultipleAllStrategy(bars, event_queue, [
                ta.TAMin(bars, event_queue, ta.rsi, 14, 7, OrderPosition.SELL),
                ta.TAMin(bars, event_queue, ta.cci, 20, 7, OrderPosition.SELL),
            ])  # sell
        ]),
    ], "StratValue")

    dcf_value_growth = MultipleAllStrategy(bars, event_queue, [
        fundamental.DCFSignal(bars, event_queue, 1.0, 5.0),
        statistics.EitherSide(bars, event_queue, 100, 25),
        MultipleAnyStrategy(bars, event_queue, [
            MultipleAllStrategy(bars, event_queue, [
                MultipleAnyStrategy(bars, event_queue, [
                    fundamental.FundAtLeast(
                        bars, event_queue, 'revenueGrowth', 0.05, order_position=OrderPosition.BUY),
                    fundamental.FundAtLeast(
                        bars, event_queue, 'operatingIncomeGrowth', 0.1, order_position=OrderPosition.BUY),
                ]),
                fundamental.FundAtLeast(bars, event_queue, 'returnOnEquity',
                                        0.03, order_position=OrderPosition.BUY),
                # fundamental.FundAtLeast(bars, event_queue, 'roic',
                #     0, order_position=OrderPosition.BUY),
                ta.TAMax(bars, event_queue, ta.rsi, 14, 7, OrderPosition.BUY),
            ]),
            ta.TAMin(bars, event_queue, ta.rsi, 14, 7, OrderPosition.SELL),
        ]),
    ], "DcfValueGrowth")

    high_beta_strat = ComplexHighBeta(bars, event_queue, ETF_LIST,
                                      index_strategy=MultipleAllStrategy(bars, event_queue, [
                                          statistics.ExtremaBounce(
                                              bars, event_queue, short_period=5, long_period=80, percentile=20),
                                          MultipleAnyStrategy(bars, event_queue, [
                                              ta.TAMax(
                                                  bars, event_queue, ta.rsi, 14, 7, OrderPosition.BUY),
                                              ta.TAMin(
                                                  bars, event_queue, ta.rsi, 14, 7, OrderPosition.SELL),
                                          ])
                                      ]), corr_days=60, corr_min=0.85, description="HighBetaValue")

    strategy = MultipleSendAllStrategy(bars, event_queue, [
        # profitable.value_extremaTA(bars, event_queue),
        MultipleAllStrategy(bars, event_queue, [
            OneSidedOrderOnly(bars, event_queue, OrderPosition.SELL),
            profitable.momentum_with_TACross(bars, event_queue)
        ]),
        dcf_value_growth,
        high_beta_strat,
        # profitable.bounce_ta(bars, event_queue),
        # profitable.comprehensive_longshort(bars, event_queue),
    ])  # , dcf_value_growth, strat_value

    rebalance_strat = RebalanceLogicalAny(bars, event_queue, [
        # SellLosersHalfYearly(bars, event_queue),
        RebalanceYearly(bars, event_queue),
        SellWinnersQuarterly(bars, event_queue)
    ])
    port = PercentagePortFolio(bars, event_queue, order_queue,
                               percentage=0.05,
                               portfolio_name=(
                                   args.name if args.name != "" else "loop"),
                               mode='asset',
                               expires=1,
                               rebalance=rebalance_strat,
                               initial_capital=INITIAL_CAPITAL
                               )
    broker = SimulatedBroker(bars, port, event_queue, order_queue, gatekeepers=[
        NoShort(bars), EnoughCash(bars), MaxPortfolioValuePerInst(bars, 0.20), PremiumLimit(bars, 150.0)
    ])
    backtest(bars, event_queue, order_queue,
             strategy, port, broker, start_date=start_date, show_plot=args.num_runs == 1, initial_capital=INITIAL_CAPITAL)
    port.write_all_holdings()


if __name__ == "__main__":
    INITIAL_CAPITAL = 3000
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
