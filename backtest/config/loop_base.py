from datetime import timedelta
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from trading.portfolio.portfolio import FixedTradeValuePortfolio
from trading.portfolio.rebalance import NoRebalance, RebalanceMonthly
from trading.strategy.complex.correlation import PairTrading, InvPairTrading
from trading.utilities.enum import OrderType


def get_config() -> dict:
    d = {
        # MANDATORY
        "credentials_fp": Path(os.environ["WORKSPACE_ROOT"]) / "real_credentials.json",
        "data_config_fp": Path("/mnt/HDD/Ivan/projects/IBKRDataStream/config/historical_bars.json"),
        "name": "test",
        "save_portfolio": False,
        "load_portfolio": False,
        "data_provider": None,
        "strategy": None,
        "portfolio": None,  # updated below
        "gk": [],
        "initial_capital": 10_000,
        # OPTIONAL
    }
    strategy = InvPairTrading(
        100, pairs=[("QQQ", "QID"), ("IWM", "RWM")], price_margin_perc=1e-6,
        min_update_freq=timedelta(minutes=2), sd_deviation=3
    )

    # eng = create_engine(os.environ["DB_URL"])
    port = FixedTradeValuePortfolio(
        trade_value=5_000,
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
