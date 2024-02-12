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
from trading.portfolio.portfolio import FixedTradeValuePortfolio, PercentagePortFolio, SignalDefinedPosPortfolio
from trading.data.dataHandler import HistoricCSVDataHandler, OptionDataHandler
from trading.strategy.basic import BuyAndHoldStrategy
from trading.strategy.complex.option_strategy import SellStrangle, SellStrangleMLModel
from trading.strategy.fairprice.feature import RelativeCCI, RelativeRSI, TradeImpulseBase, TrendAwareFeatureEMA
from trading.strategy.fairprice.margin import AsymmetricPercentageMargin
from trading.strategy.fairprice.strategy import FairPriceStrategy
from trading.strategy.personal import profitable
from trading.strategy.statmodels.models import EquityPrediction
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
        start_ms = generate_start_date_in_ms(2019 if args.inst_type == "equity" and args.frequency == "day" else 2021, 2023)
    inst_days = random.randint(250, 600) if args.inst_type == "equity" else random.randint(60, 250)
    # end anytime between 250 - 600 days later
    end_ms = int(start_ms + inst_days * 8.64e7 * (1.0 if args.frequency == "day" else 0.25))
    print(start_ms, end_ms)
    universe_list = read_universe_list(args.universe)
    symbol_list = random.sample(universe_list, min(150 if args.inst_type == "equity" else 75, len(universe_list)))
    symbol_list = random.sample(universe_list, 20)
    if args.inst_type == "options":
        assert args.frequency != "day", "current option strategy does not support day"
        bars = OptionDataHandler(symbol_list, creds, start_ms, end_ms, frequency_type=args.frequency)
    else:
        bars = HistoricCSVDataHandler(symbol_list, creds,
                                    start_ms=start_ms,
                                    end_ms=end_ms,
                                    frequency_type=args.frequency
                                    )
    model_dir = Path(os.environ["DATA_DIR"]) / "models"
    if args.inst_type == "equity":
        STRATEGY = "ML" # TA or ML
        rebalance_strat = RebalanceLogicalAny(bars, [
            SellWinnersQuarterly(bars, 0.26) if args.frequency == "day" else SellWinnersMonthly(bars, 0.125),
            SellLosersQuarterly(bars, 0.14) if args.frequency == "day" else SellLosersMonthly(bars, 0.075),
        ])
        if STRATEGY == "TA":
            period = 15     # period to calculate algo
            ta_period = 14  # period of calculated values seen 
            feature = TrendAwareFeatureEMA(period + ta_period // 2) + RelativeRSI(ta_period, 10) + RelativeCCI(ta_period, 12) + TradeImpulseBase(period // 2)
            margin = AsymmetricPercentageMargin((0.03, 0.03) if args.frequency == "day" else (0.016, 0.01))
            strategy = FairPriceStrategy(bars, feature, margin, period + ta_period)
        elif STRATEGY == "ML":
            period = 25
            strategy = EquityPrediction(
                model_dir / "equity_prediction_perc_min.lgb.txt",
                model_dir / "equity_prediction_perc_max.lgb.txt",
                period,  # lookback
                lookahead=20,   # in days
                frequency=args.frequency,
                min_move_perc=0.05,  # 0.5%
                description="EquityRangeML"
            )
    else:
        # strategy = SellStrangle(bars)
        strategy = SellStrangleMLModel(
            bars, 
            model_dir / "option_short_strangle_low.lgb.txt",
            model_dir / "option_short_strangle_high.lgb.txt")
        # "option_short_strangle_high_model.joblib"
        rebalance_strat = NoRebalance(bars)
    port = FixedTradeValuePortfolio(trade_value=800,
                            max_qty=20 if args.inst_type == "equity" else 2,
                            portfolio_name=(
                                args.name if args.name != "" else "loop"),
                            expires=1,
                            rebalance=rebalance_strat,
                            order_type=OrderType.LIMIT if args.inst_type == "equity" else OrderType.MARKET,
                            initial_capital=INITIAL_CAPITAL
                            )

    broker = SimulatedBroker(bars, port, gatekeepers=[
        EnoughCash(), MaxPortfolioPercPerInst(bars, 0.25)
        # PremiumLimit(150), MaxInstPosition(3), MaxPortfolioPosition(24) # test real scenario since I'm poor
    ] + [NoShort()] if args.inst_type == "equity" else [])
    bt = Backtest(bars, strategy, port, broker, args)
    bt.run(live=False)

    args.start_ms = start_ms
    args.end_ms = end_ms
    
    plot_index_benchmark(args, ['SPY'], "BuyAndHoldIndex")
    if args.inst_type == "equity":
        plot_index_benchmark(args, symbol_list, "BuyAndHoldStrategy")

    if args.name:
        port.write_curr_holdings()
        port.write_all_holdings()

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
    with fut.ProcessPoolExecutor(4) as e:
        processes = [e.submit(main, creds) for i in range(args.num_runs)]
        processes = [p.result() for p in processes]
