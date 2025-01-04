from datetime import timedelta
import os
from pathlib import Path

import pandas as pd

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
        "is_live_acct": False,
        # OPTIONAL
    }

    strategy = PairTrading(30, pairs=[("SPY", "VOO"), ("QQQ", "QQQM")], price_margin_perc=1e-5,
                           min_update_freq=timedelta(minutes=2), sd_deviation=2)
    eng = get_db_engine()
    df_warmup = pd.read_sql("""
        SELECT * FROM ibkr.market_data_bars_uniq
        WHERE frequency = '15 mins'
            and to_timestamp(timestamp / 1000) > date('2024-06-01')
        order by timestamp;
    """, eng)
    strategy.warmup(df_warmup)
    print(len(strategy.pairs_info["VOO"].past_ratios), strategy.pairs_info["VOO"].p1_update_time)

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
