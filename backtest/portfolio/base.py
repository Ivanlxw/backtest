# portfolio.py

import datetime
import numpy as np
import pandas as pd
import queue

from abc import ABCMeta, abstractmethod
from math import fabs

from backtest.event import FillEvent, OrderEvent
from backtest.performance import create_sharpe_ratio, create_drawdowns
from backtest.portfolio.rebalance.base import NoRebalance

class Portfolio(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def update_signal(self, event):
        raise NotImplementedError("Should implement update_signal()")

    @abstractmethod
    def update_fill(self,event):
        """
        Updates portfolio current positions and holdings 
        from FillEvent
        """
        raise NotImplementedError("Should implement update_fill()")

class NaivePortfolio(Portfolio):
    def __init__(self, bars, events, order_events, stock_size, initial_capital=100000.0, rebalance=None):
        """ 
        Parameters:
        bars - The DataHandler object with current market data.
        events - The Event Queue object.
        start_date - The start date (bar) of the portfolio.
        initial_capital - The starting capital in USD.
        """
        self.bars = bars  
        self.events = events
        self.order_events = order_events
        self.symbol_list = self.bars.symbol_list
        self.start_date = self.bars.start_date
        self.initial_capital = initial_capital
        self.qty = stock_size

        self.all_positions = self.construct_all_positions()
        self.current_positions = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        self.all_holdings = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()

        self.rebalance = rebalance if rebalance is not None else NoRebalance()

    def construct_all_positions(self,):
        """
        Returns a list of dictionary, that shows the positions at 
        every timestep
        """
        d = dict( (k,v) for k,v in [(s,0) for s in self.symbol_list])
        d['datetime'] = datetime.datetime.strptime(self.start_date, '%Y-%m-%d')
        return [d]
    
    def construct_all_holdings(self,):
        """
        Constructs the holdings list using the start_date
        to determine when the time index will begin.
        """
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['datetime'] = datetime.datetime.strptime(self.start_date, '%Y-%m-%d')
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]

    def construct_current_holdings(self, ):
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        return d

    def update_timeindex(self, event):
        bars = {}
        for sym in self.symbol_list:
            bars[sym] = self.bars.get_latest_bars(sym, N=1)
        
        ## update positions
        dp = dict( (k,v) for k, v in [(s,0) for s in self.symbol_list ])
        dp['datetime'] = bars[self.symbol_list[0]][0][1]

        for s in self.symbol_list:
            dp[s] = self.current_positions[s]
        
        ## add historical info of current timeframe
        ## positions
        self.all_positions.append(dp)

        ## update holdings
        dh = dict((k,v) for k, v in [(s,0) for s in self.symbol_list])
        dh['datetime'] = bars[self.symbol_list[0]][0][1]
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] = self.current_holdings['commission']
        dh['total'] = self.current_holdings['cash']

        for s in self.symbol_list:
            ## position size * close price
            market_val = self.current_positions[s] * bars[s][0][5]
            dh[s] = market_val
            dh['total'] += market_val
        
        ## append current holdings
        self.all_holdings.append(dh)
        self.rebalance.rebalance(self.symbol_list, self.all_holdings)

    def update_positions_from_fill(self, fill):
        """
        Takes a FillEvent object and updates the position matric to 
        reflect new position
        """

        fill_dir = 0
        if fill.direction == "BUY":
            fill_dir = 1
        elif fill.direction == "SELL":
            fill_dir = -1

        self.current_positions[fill.symbol] += fill_dir*fill.quantity

    def update_holdings_from_fill(self, fill: FillEvent):
        fill_dir = 0
        if fill.direction == "BUY":
            fill_dir = 1
        elif fill.direction == "SELL":
            fill_dir = -1  

        close_price = self.bars.get_latest_bars(fill.symbol)[0][5] ## close price
        cash = fill_dir * close_price * fill.quantity
        self.current_holdings[fill.symbol] += fill_dir*fill.quantity
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cash + fill.commission)
    
    def update_fill(self, event):
        if event.type == "FILL":
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)

    def generate_naive_order(self, signal, size):
        """
        takes a signal to long or short an asset and then sends an order 
        of size=size of such an asset
        """
        order = None
        symbol = signal.symbol
        direction = signal.signal_type

        cur_quantity = self.current_positions[symbol]
        order_type = 'MKT'

        if direction == 'EXIT':
            if cur_quantity > 0:
                order = OrderEvent(symbol, order_type, cur_quantity, 'SELL')
            else:
                order = OrderEvent(symbol, order_type, -cur_quantity, 'BUY')            
        elif direction == 'LONG':
            if cur_quantity < 0:
                order = OrderEvent(symbol, order_type, size-cur_quantity, 'BUY')
            else:
                order = OrderEvent(symbol, order_type, size, 'BUY')
        elif direction == 'SHORT':
            if cur_quantity > 0:
                order = OrderEvent(symbol, order_type, size+cur_quantity, 'SELL')
            else:
                order = OrderEvent(symbol, order_type, size, 'SELL')
        return order
    
    def generate_order(self, signal) -> OrderEvent:
        return self.generate_naive_order(signal, self.qty)
    
    def update_signal(self, event):
        if event.type == 'SIGNAL':
            mkt_price = self.bars.get_latest_bars(event.symbol)[0][5]
            order_event = self.generate_order(event)
            if order_event is not None: 
                order_value = fabs(order_event.quantity * mkt_price)
                if (self.current_holdings["cash"] > order_value and order_event.direction == 'BUY') or \
                    (self.all_holdings[-1]["total"] > order_value and order_event.direction == 'SELL'): 
                    ## ExecutionEvent should execute previous EOD orders
                    self.order_events.put(order_event)

    def create_equity_curve_df(self):
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('datetime', inplace=True)
        curve['equity_returns'] = curve['total'].pct_change()
        curve['equity_curve'] = (1.0+curve['equity_returns']).cumprod()
        curve['liquidity_returns'] = curve['cash'].pct_change()
        curve['liquidity_curve'] = (1.0+curve['liquidity_returns']).cumprod()
        self.equity_curve = curve.dropna()

    def output_summary_stats(self):
        total_return = self.equity_curve['equity_curve'][-1]
        returns = self.equity_curve['equity_returns']
        pnl = self.equity_curve['equity_curve']

        sharpe_ratio = create_sharpe_ratio(returns)
        max_dd, dd_duration = create_drawdowns(pnl)

        stats = [("Total Return", "%0.2f%%" % ((total_return - 1.0) * 100.0)),
                 ("Sharpe Ratio", "%0.2f" % sharpe_ratio),
                 ("Max Drawdown", "%0.2f%%" % (max_dd * 100.0)),
                 ("Drawdown Duration", "%d" % dd_duration),
                 ("Lowest point" , "%0.2f%%" % (np.amin(self.equity_curve["equity_curve"])*100)),
                 ("Lowest Cash", "%f" % (np.amin(self.equity_curve["cash"])))]
        return stats

    def get_backtest_results(self,fp):
        if self.equity_curve == None:
            raise Exception("Error: equity_curve is not initialized.")
        self.equity_curve.to_csv(fp)

class PercentagePortFolio(NaivePortfolio):
    def __init__(self, bars, events, order_events, percentage, initial_capital=100000.0, rebalance=None, mode='cash'):
        super().__init__(bars, events, order_events, stock_size=0, initial_capital=initial_capital, rebalance=rebalance)
        if mode not in ('cash', 'asset'):
            raise Exception('mode options: cash | asset')
        self.mode = mode
        if percentage > 1:
            self.perc = percentage / 100
        else:
            self.perc = percentage
    
    def generate_order(self, signal):
        mkt_price = self.bars.get_latest_bars(signal.symbol)[0][5]
        if mkt_price == 0:
            return
        size = int(self.current_holdings["cash"] * self.perc / mkt_price) if self.mode == 'cash' \
            else int(self.all_holdings[-1]["total"] * self.perc / mkt_price)
        return self.generate_naive_order(signal, size)
