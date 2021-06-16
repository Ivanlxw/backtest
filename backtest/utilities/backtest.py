import queue
import os
import copy
import matplotlib.pyplot as plt

from backtest.broker import SimulatedBroker
from backtest.portfolio.portfolio import PercentagePortFolio
from backtest.strategy.naive import BuyAndHoldStrategy
from backtest.utilities.enums import OrderType
from backtest.utilities.utils import _backtest_loop, _life_loop


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
        benchmark_bars = copy.copy(bars)
        plot_benchmark("data/stock_list.txt",
                       symbol_list=symbol_list,
                       benchmark_bars=benchmark_bars)

        plt.legend()
        plt.show()


def plot_benchmark(stock_list_fp, symbol_list, benchmark_bars, freq="daily"):
    event_queue = queue.LifoQueue()
    order_queue = queue.Queue()
    benchmark_bars.events = event_queue
    # Declare the components with relsspective parameters
    strategy = BuyAndHoldStrategy(benchmark_bars, event_queue)
    port = PercentagePortFolio(benchmark_bars, event_queue, order_queue,
                               percentage=1/len(symbol_list), portfolio_name="benchmark",
                               mode='asset', order_type=OrderType.MARKET)
    broker = SimulatedBroker(benchmark_bars, port, event_queue, order_queue)

    _backtest_loop(benchmark_bars, event_queue,
                   order_queue, strategy, port, broker)
