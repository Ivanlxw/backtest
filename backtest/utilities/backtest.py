from trading_common.data.dataHandler import HistoricCSVDataHandler
import queue
import os
import copy
import matplotlib.pyplot as plt

from backtest.broker import SimulatedBroker
from backtest.portfolio.portfolio import PercentagePortFolio, Portfolio
from backtest.strategy.naive import BuyAndHoldStrategy
from backtest.utilities.utils import _backtest_loop, _life_loop
from trading_common.utilities.enum import OrderType
from trading_common.utilities.constants import benchmark_ticker

def backtest(symbol_list,
             bars, event_queue, order_queue,
             strategy, port, broker,
             start_date=None,
             plot_trade_prices: bool = False,
             loop_live: bool = False):
    if not loop_live and start_date is None:
        raise Exception("If backtesting, start_date is required.")

    if loop_live:
        _life_loop(bars, event_queue, order_queue, strategy, port, broker)
    else:
        _backtest_loop(bars, event_queue, order_queue, strategy, port, broker)
        benchmark_strat_bars = copy.copy(bars)
        plot_benchmark(symbol_list=symbol_list,
                       portfolio_name="benchmark_strat", benchmark_bars=benchmark_strat_bars)
        plot_benchmark(symbol_list=[benchmark_ticker],
                       portfolio_name="benchmark_index", benchmark_bars=None, start_date=start_date)

        plt.legend()
        plt.show()


def plot_benchmark(symbol_list, portfolio_name, benchmark_bars=None, freq="daily", start_date=None):
    event_queue = queue.LifoQueue()
    order_queue = queue.Queue()
    if benchmark_bars is None and start_date is None:
        raise Exception("If benchmark_bars is None, start_date cannot be None")
    elif benchmark_bars is None and start_date is not None:
        # create new benchmark_bars with index only
        benchmark_bars = HistoricCSVDataHandler(event_queue,
                                                csv_dir=os.path.join(os.path.dirname(
                                                    __file__), "../../data/data/daily"),
                                                symbol_list=symbol_list,
                                                start_date=start_date,
                                                fundamental=False
                                                )
    benchmark_bars.events = event_queue
    # Declare the components with relsspective parameters
    strategy = BuyAndHoldStrategy(benchmark_bars, event_queue)
    port = PercentagePortFolio(benchmark_bars, event_queue, order_queue,
                               percentage=1/len(symbol_list), portfolio_name=portfolio_name,
                               mode='asset', order_type=OrderType.MARKET)
    broker = SimulatedBroker(benchmark_bars, port, event_queue, order_queue)

    _backtest_loop(benchmark_bars, event_queue,
                   order_queue, strategy, port, broker)
