import queue
import os
import matplotlib.pyplot as plt  

from backtest.broker import SimulatedBroker
from backtest.data.dataHandler import HistoricCSVDataHandler
from backtest.portfolio.base import PercentagePortFolio
from backtest.strategy.naive import BuyAndHoldStrategy
from backtest.utilities.enums import OrderType
from backtest.utilities.utils import _backtest_loop

def backtest(symbol_list, 
             bars, event_queue, order_queue, 
             strategy, port, broker, 
             start_date=None,
             plot_trade_prices:bool=False,
             loop_live:bool=False):
    if not loop_live and start_date is None:
        raise Exception("If backtesting, start_date is required.")
         
    plotter = _backtest_loop(bars, event_queue, order_queue, strategy, port, broker, loop_live=loop_live)

    if not loop_live:
        plot_benchmark("data/stock_list.txt", \
            symbol_list=symbol_list, \
            start_date = start_date)

        plt.legend()
        plt.show()

    if plot_trade_prices:
        plotter.plot_trade_prices()

def plot_benchmark(stock_list_fp, symbol_list, start_date, end_date:str=None, freq="daily"):
    event_queue = queue.LifoQueue()
    order_queue = queue.Queue()
    # Declare the components with relsspective parameters
    csv_dir = os.path.dirname(os.getcwd() + "/" +stock_list_fp) + f"/data/{freq}" 
    bars = HistoricCSVDataHandler(event_queue, csv_dir=csv_dir,
                                            symbol_list=symbol_list,
                                            start_date=start_date,
                                            end_date=end_date)
    strategy = BuyAndHoldStrategy(bars, event_queue)
    port = PercentagePortFolio(bars, event_queue, order_queue, 
                               percentage=1/len(symbol_list), mode='asset',
                               order_type=OrderType.MARKET)
    broker = SimulatedBroker(bars, event_queue, order_queue)

    _backtest_loop(bars, event_queue, order_queue, strategy, port, broker)