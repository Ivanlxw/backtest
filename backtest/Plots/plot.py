from backtest.utilities.enums import OrderPosition
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

from backtest.portfolio.base import Portfolio
from backtest.data.dataHandler import DataHandler

class Plot:
    def __init__(self, port:Portfolio) -> None:
        self.port = port
        sns.set()
        sns.set_style('darkgrid')

    def _plot_equity_curve(self):
        plt.subplot(2,1,1)
        plt.title("Equity curve")
        plt.plot(self.port.equity_curve['equity_curve'], label="strat_eq")
        plt.plot(self.port.equity_curve['liquidity_curve'], label="strat_cash")
        plt.subplot(2,1,2)
        plt.title("Assets over time")
        plt.plot(self.port.equity_curve["total"], label="strat_total")
        plt.plot(self.port.equity_curve['cash'], label="strat_cash")
        plt.tight_layout()

    def plot(self):
        self._plot_equity_curve()

class PlotTradePrices(Plot):
    def __init__(self, port:Portfolio, bars:DataHandler) -> None:
        super().__init__(port)
        self.bars = bars
        self.signals = np.array(port.trade_details)
    
    def plot_indi_equity_value(self, ):
        for sym in self.port.symbol_list:
            plt.plot(self.port.equity_curve[sym])
            plt.title(f"Market value of {sym}")
        plt.show()

    def plot_trade_prices(self):
        '''
        A look at where trade happens
        '''
        for idx,ticker in enumerate(self.port.symbol_list):
            buy_signals = self.signals[np.where((self.signals[:,0] == ticker) & (self.signals[:,-1] == OrderPosition.BUY))]
            sell_signals = self.signals[np.where((self.signals[:,0] == ticker) & (self.signals[:,-1] == OrderPosition.SELL))]
            self.bars.raw_data[ticker].index = self.bars.raw_data[ticker].index.map(lambda x: datetime.strptime(x, '%Y-%m-%d'))

            plt.subplot(len(self.port.symbol_list), 1, idx+1)
            plt.plot(self.bars.raw_data[ticker]['close'])
            plt.scatter(buy_signals[:,1], buy_signals[:,5], c='g', marker="x")
            plt.scatter(sell_signals[:,1], sell_signals[:,5], c='r', marker="x")
            plt.title(f"Trade prices for {ticker}")
        plt.tight_layout()
        plt.show()