import os
from pathlib import Path
from trading.broker.gatekeepers import EnoughCash
from trading.portfolio.portfolio import FixedTradeValuePortfolio
from trading.portfolio.rebalance import NoRebalance, RebalanceMonthly
from trading.strategy.complex.correlation import PairTrading
from trading.utilities.enum import OrderType


def get_config() -> dict:
    base_data_dir = Path(os.environ["DATA_DIR"])
    d = {
        # MANDATORY
        "credentials_fp": Path(os.environ["WORKSPACE_ROOT"]) / "real_credentials.json",
        "data_config_fp": Path("/mnt/HDD/Ivan/projects/IBKRDataStream/config/historical_bars.json"),
        # "universe": [base_data_dir / "universe/etf.txt"],
        "name": "test",
        "save_portfolio": False,
        "load_portfolio": False,
        "data_provider": None,
        "strategy": None,
        "portfolio": None,  # updated below
        "gk": [],
        "initial_capital": 100_000,
        # OPTIONAL
    }
    # , mean_deviation_perc=0.01

    strategy = PairTrading(40, pairs=[("SPY", "VOO"), ("QQQ", "QQQM")], sd_deviation=3)
    port = FixedTradeValuePortfolio(
        trade_value=25_000,
        max_qty=100_000,
        portfolio_name=d["name"],
        expires=1,
        rebalance=NoRebalance(),
        order_type=OrderType.LIMIT,
        initial_capital=d['initial_capital'],
        load_portfolio_details=d["load_portfolio"],
    )
    d["strategy"] = strategy
    d["portfolio"] = port
    return d
