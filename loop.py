import os
import random
import logging
import os
import concurrent.futures as fut
from pathlib import Path

import matplotlib.pyplot as plt

from backtest.utilities.backtest import Backtest
from backtest.utilities.utils import generate_start_date_in_ms, parse_args, load_credentials, read_universe_list
from trading.broker.broker import SimulatedBroker
from trading.broker.gatekeepers import EnoughCash, MaxPortfolioPercPerInst, NoShort
from trading.portfolio.rebalance import NoRebalance, RebalanceLogicalAny, RebalanceYearly, SellLosersMonthly, SellLosersQuarterly, SellWinnersMonthly, SellWinnersQuarterly
from trading.portfolio.portfolio import FixedTradeValuePortfolio, PercentagePortFolio
from trading.data.dataHandler import HistoricCSVDataHandler
from trading.strategy.basic import BuyAndHoldStrategy
from trading.strategy.fairprice.feature import RelativeCCI, RelativeRSI, TradeImpulseBase, TradePressureEma, TrendAwareFeatureEMA
from trading.strategy.fairprice.margin import AsymmetricPercentageMargin
from trading.strategy.fairprice.strategy import FairPriceStrategy
from trading.strategy.personal import profitable
from trading.utilities.enum import OrderType


def plot_index_benchmark(args, symbol_list, portfolio_name):
    bars = HistoricCSVDataHandler(symbol_list, creds,
                                    start_ms=args.start_ms,
                                    end_ms=args.end_ms,
                                    frequency_type=args.frequency
                                    )
    rebalance=NoRebalance(bars)
    port = PercentagePortFolio(1/len(symbol_list), rebalance, initial_capital=INITIAL_CAPITAL,
                               portfolio_name=portfolio_name,
                               mode='asset', order_type=OrderType.MARKET)
    broker = SimulatedBroker(bars, port)
    strategy = BuyAndHoldStrategy(bars)
    bt = Backtest(bars, strategy, port, broker, args)
    bt.run(live=False)


def main(creds):
    # YYYY-MM-DD
    if args.start_ms is not None:
        start_ms = args.start_ms
    else:
        start_ms = generate_start_date_in_ms(2019, 2022)
    end_ms = int(start_ms + random.randint(250, 700) * 8.64e7 * (1.0 if args.frequency == "day" else 0.25))  # end anytime between 200 - 800 days later
    print(start_ms, end_ms)
    universe_list = read_universe_list(args.universe)
    symbol_list = set(random.sample(universe_list, min(
        80, len(universe_list))))
    bars = HistoricCSVDataHandler(symbol_list, creds,
                                  start_ms=start_ms,
                                  end_ms=end_ms,
                                  frequency_type=args.frequency
                                  )
    # strategy = profitable.trading_idea_two(bars, event_queue)
    period = 15     # period to calculate algo
    ta_period = 14  # period of calculated values seen 
    feature = TrendAwareFeatureEMA(period + ta_period // 2) + RelativeRSI(ta_period, 10) + RelativeCCI(ta_period, 12) #  + TradeImpulseBase(period // 2)
    margin = AsymmetricPercentageMargin((0.03, 0.03) if args.frequency == "day" else (0.016, 0.01))
    strategy = FairPriceStrategy(bars, feature, margin, period + ta_period)
    rebalance_strat = RebalanceLogicalAny(bars, [
        RebalanceYearly(bars),
        SellWinnersQuarterly(bars, 0.26) if args.frequency == "day" else SellWinnersMonthly(bars, 0.125),
        SellLosersQuarterly(bars, 0.14) if args.frequency == "day" else SellLosersMonthly(bars, 0.075), 
    ])
    port = FixedTradeValuePortfolio(trade_value=1200,
                                    max_qty=10,
                                    portfolio_name=(
                                        args.name if args.name != "" else "loop"),
                                    expires=1,
                                    rebalance=rebalance_strat,
                                    initial_capital=INITIAL_CAPITAL
                                    )
    broker = SimulatedBroker(bars, port, gatekeepers=[
        NoShort(), EnoughCash(),
        # PremiumLimit(150), MaxInstPosition(3), MaxPortfolioPosition(24) # test real scenario since I'm poor
        MaxPortfolioPercPerInst(bars, 0.2)
    ])
    bt = Backtest(bars, strategy, port, broker, args)
    bt.run(live=False)

    args.start_ms = start_ms
    args.end_ms = end_ms
    
    plot_index_benchmark(args, ['SPY'], "BuyAndHoldIndex")
    # plot_index_benchmark(args, symbol_list, "BuyAndHoldStrategy")

    if bt.show_plot:
        plt.legend()
        plt.show()


if __name__ == "__main__":
    INITIAL_CAPITAL = 10000
    args = parse_args()
    creds = load_credentials(args.credentials)

    if args.name != "":
        logging.basicConfig(filename=Path(os.environ["DATA_DIR"]) /
                            f"logging/{args.name}.log", level=logging.INFO, force=True)
    processes = []
    with fut.ProcessPoolExecutor(4) as e:
        for i in range(args.num_runs):
            processes.append(e.submit(main, creds))
    processes = [p.result() for p in processes]