# portfolio.py

import datetime
import numpy as np
import pandas as pd
import queue

from abc import ABCMeta, abstractmethod
from math import floor, fabs

from event import FillEvent, OrderEvent
from performance import create_sharpe_ratio, create_drawdowns


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
    def __init__(self, bars, events, start_date, initial_capital=100000.0):
        """ 
        Parameters:
        bars - The DataHandler object with current market data.
        events - The Event Queue object.
        start_date - The start date (bar) of the portfolio.
        initial_capital - The starting capital in USD.
        """
        self.bars = bars  
        self.events = events
        self.symbol_list = self.bars.symbol_list
        self.start_date = start_date
        self.initial_capital = initial_capital

        self.all_positions = self.construct_all_positions()
        self.current_positions = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )

        self.all_holdings = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()

    def construct_all_positions(self,):
        """
        Returns a list of dictionary, that shows the positions at 
        every timestep
        """
        d = dict( (k,v) for k,v in [(s,0) for s in self.symbol_list])
        d['datetime'] = self.start_date
        return [d]
    
    def construct_all_holdings(self,):
        """
        Constructs the holdings list using the start_date
        to determine when the time index will begin.
        """
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]


    def construct_current_holdings(self, ):
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
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
        dh = dict((k,v) for k, v in [(s,0) for s in self.symbol_list ])
        dh['datetime'] = bars[self.symbol_list[0]][0][1]
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] = self.current_holdings['commission']
        dh['total'] = self.current_holdings['cash']

        for s in self.symbol_list:
            ## position size * close price
            # print(bars[s])
            market_val = self.current_positions[s] * bars[s][0][5]
            dh[s] = market_val
            dh['total'] += market_val
        
        ## append current holdings
        self.all_holdings.append(dh)

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

        fill_cost = self.bars.get_latest_bars(fill.symbol)[0][5] ## close price
        cost = fill_dir * fill_cost * fill.quantity
        self.current_holdings[fill.symbol] += cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)
    
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
        # strength = signal.strength

        cur_quantity = self.current_positions[symbol]
        order_type = 'MKT'

        if direction == 'REVERSE' and cur_quantity < 0:
            order = OrderEvent(symbol, order_type, size*2, 'BUY')
        elif direction == 'REVERSE' and cur_quantity > 0:
            order = OrderEvent(symbol, order_type, size*2, 'SELL')
        elif direction == 'EXIT' and cur_quantity > 0:
            order = OrderEvent(symbol, order_type, cur_quantity, 'SELL')
        elif direction == 'EXIT' and cur_quantity < 0:
            order = OrderEvent(symbol, order_type, fabs(cur_quantity), 'BUY')
        elif direction == 'LONG':
            order = OrderEvent(symbol, order_type, size, 'BUY')
        elif direction == 'SHORT':
            order = OrderEvent(symbol, order_type, size, 'SELL')
        return order
    
    def update_signal(self, event):
        if event.type == 'SIGNAL':
            qty = 100
            mkt_price = self.bars.get_latest_bars(event.symbol)[0][5]
            order_event = self.generate_naive_order(event, qty)
            if self.current_holdings["cash"] > (order_event.quantity * mkt_price): 
                self.events.put(order_event)

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
        if self.equity_curve != None:
            self.equity_curve.to_csv(fp)
        else:
            raise Exception("Error: equity_curve is not initialized.")

class PercentagePortFolio(NaivePortfolio):
    def __init__(self, bars, events, start_date, percentage, initial_capital=100000.0):
        super().__init__(bars, events, start_date, initial_capital=100000.0)
        if percentage > 1:
            self.perc = percentage / 100
        else:
            self.perc = percentage
    
    def generate_perc_order(self, signal, mkt_price):
        size = int(self.current_holdings["cash"] * self.perc / mkt_price)
        return self.generate_naive_order(signal, size)

    def update_signal(self, event):
        if event.type == 'SIGNAL':
            mkt_price = self.bars.get_latest_bars(event.symbol)[0][5]
            order_event = self.generate_perc_order(event, mkt_price)
            if order_event == None: 
                return
            if self.current_holdings["cash"] > (order_event.quantity * mkt_price): 
                self.events.put(order_event)