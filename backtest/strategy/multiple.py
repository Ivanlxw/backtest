from backtest.strategy.naive import Strategy

class MultipleStrategy(Strategy):
    def __init__(self, strategies: Strategy) -> None:
        self.strategies = strategies
    
    def calculate_signals(self, event):
        for strategy in self.strategies:
            strategy.calculate_signals(event)