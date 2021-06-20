# portfolio.py

from backtest.portfolio.strategy import DefaultOrder
from trading_common.utilities.enum import OrderPosition, OrderType
import numpy as np
import pandas as pd
from datetime import timedelta
from abc import ABCMeta, abstractmethod

from trading_common.event import FillEvent, OrderEvent, SignalEvent
from backtest.performance import create_sharpe_ratio, create_drawdowns
from backtest.portfolio.rebalance import NoRebalance

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
    def __init__(self, bars, events, order_queue, stock_size, portfolio_name, 
                initial_capital=100000.0, order_type=OrderType.LIMIT, portfolio_strategy=DefaultOrder, 
                rebalance=None, expires:int=1, ):
        """ 
        Parameters:
        bars - The DataHandler object with current market data.
        events - The Event Queue object.
        start_date - The start date (bar) of the portfolio.
        initial_capital - The starting capital in USD.
        """
        self.bars = bars  
        self.events = events
        self.order_queue = order_queue
        self.symbol_list = self.bars.symbol_list
        if type(self.bars.start_date) == str:
            self.start_date = pd.Timestamp(self.bars.start_date)
        else:
            self.start_date = self.bars.start_date
        self.initial_capital = initial_capital
        self.qty = stock_size
        self.expires = expires
        self.name = portfolio_name
        self.current_holdings = self.construct_current_holdings()
        self.all_holdings = self.construct_all_holdings()
        self.order_type = order_type
        self.portfolio_strategy = portfolio_strategy(self.bars, self.current_holdings, self.order_type)
        self.rebalance = rebalance(self.events, self.bars) if rebalance is not None else NoRebalance()

    def construct_all_holdings(self,):
        """
        Constructs the holdings list using the start_date
        to determine when the time index will begin.
        self. all_holdings = list({
            symbols: market_value,
            datetime, 
            cash,
            daily_commission,
            total_asset,
        })
        """
        d = dict((s, 0.0) for s in self.symbol_list)
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]

    def construct_current_holdings(self, ):
        d = dict( (s, {
            'quantity': 0.0,
            'last_traded': None,
            'last_trade_price': None
        }) for s in self.symbol_list )
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['datetime'] = self.start_date
        return d

    def update_timeindex(self, event):
        bars = {}
        for sym in self.symbol_list:
            bars[sym] = self.bars.get_latest_bars(sym, N=1)
        self.current_holdings['datetime'] = bars[self.symbol_list[0]]['datetime'][0]

        ## update holdings based off last trading day
        dh = dict( (s,0) for s in self.symbol_list )
        dh['datetime'] = self.current_holdings['datetime']
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] = self.current_holdings['commission']
        dh['total'] = self.current_holdings['cash']

        for s in self.symbol_list:
            ## position size * close price
            market_val = self.current_holdings[s]['quantity'] * (bars[s]['close'][0] if 'close' in bars[s] and len(bars[s]['close']) > 0 else 0)
            dh[s] = market_val
            dh['total'] += market_val
        
        ## append current holdings
        self.all_holdings.append(dh)
        self.current_holdings["commission"] = 0.0  # reset commission for the day
        self.rebalance.rebalance(self.symbol_list, self.current_holdings)

    def update_holdings_from_fill(self, fill: FillEvent):
        fill_dir = 0
        if fill.order_event.direction == OrderPosition.BUY:
            fill_dir = 1
        elif fill.order_event.direction == OrderPosition.SELL:
            fill_dir = -1  

        cash = fill_dir * fill.order_event.trade_price * fill.order_event.quantity
        self.current_holdings[fill.order_event.symbol]['last_traded'] = fill.order_event.date
        self.current_holdings[fill.order_event.symbol]["quantity"] += fill_dir*fill.order_event.quantity
        # latest trade price. Might need to change to avg trade price
        self.current_holdings[fill.order_event.symbol]['last_trade_price'] = fill.order_event.trade_price
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cash + fill.commission)
        
    def update_fill(self, event):
        if event.type == "FILL":
            self.update_holdings_from_fill(event)
    
    def generate_order(self, signal:SignalEvent) -> OrderEvent:
        signal.quantity = self.qty
        return self.portfolio_strategy._filter_order_to_send(signal)
    
    def _put_to_event(self, order):
        if order is not None:
            order.order_type = self.order_type
            if order.order_type == OrderType.LIMIT:
                order.expires = order.date + timedelta(days=self.expires)
                self.order_queue.put(order)
            elif order.order_type == OrderType.MARKET:
                self.events.put(order)

    def update_signal(self, event):
        if event.type == 'SIGNAL':
            order = self.generate_order(event)
            self._put_to_event(order)

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
                 ("Lowest point" , "%0.2f%%" % ((np.amin(self.equity_curve["equity_curve"]) -1) *100)),
                 ("Lowest Cash", "%f" % (np.amin(self.equity_curve["cash"])))]
        return stats

    def get_backtest_results(self,fp):
        if self.equity_curve == None:
            raise Exception("Error: equity_curve is not initialized.")
        self.equity_curve.to_csv(fp)

class PercentagePortFolio(NaivePortfolio):
    def __init__(self, bars, events, order_queue, percentage, portfolio_name, 
                initial_capital=100000.0, rebalance=None, order_type=OrderType.LIMIT, 
                portfolio_strategy=DefaultOrder, mode='cash', expires:int = 1):
        super().__init__(bars, events, order_queue, 0, portfolio_name, 
                        initial_capital=initial_capital, rebalance=rebalance, portfolio_strategy=portfolio_strategy,
                        order_type=order_type, expires=expires)
        if mode not in ('cash', 'asset'):
            raise Exception('mode options: cash | asset')
        self.mode = mode
        if percentage > 1:
            self.perc = percentage / 100
        else:
            self.perc = percentage
    
    def generate_order(self, signal:SignalEvent) -> OrderEvent:
        latest_snapshot = self.bars.get_latest_bars(signal.symbol)
        if 'close' not in latest_snapshot or latest_snapshot['close'][-1] == 0.0:
            return
        size = int(self.current_holdings["cash"] * self.perc / latest_snapshot['close'][-1]) if self.mode == 'cash' \
            else int(self.all_holdings[-1]["total"] * self.perc / latest_snapshot['close'][-1])
        signal.quantity = size
        return self.portfolio_strategy._filter_order_to_send(signal)