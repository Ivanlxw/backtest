import numpy as np
import pandas as pd

# daily periods: 252
# hourly periods: 252 * 6.5 = 1638
# minuite periods: 252 * 6.5 * 60 = 98280


def create_sharpe_ratio(returns, periods=252):
    return np.sqrt(periods) * (np.mean(returns)) / np.std(returns)


def create_drawdowns(equity_curve):
    """
    provides both the maximum drawdown and the maximum drawdown duration.
    The former is the aforementioned largest peak-to-trough drop, 
    latter is defined as the number of periods over which this drop occurs.

    Args:
    erquity_curve - pandas series representing period % returns
    """
    hwm = [0]
    eq_idx = equity_curve.index
    drawdown = pd.Series(index=eq_idx, dtype='float64')
    duration = pd.Series(index=eq_idx, dtype='float64')

    for t in range(1, len(eq_idx)):
        cur_hwm = max(hwm[t-1], equity_curve.iloc[t])
        hwm.append(cur_hwm)
        drawdown.iloc[t] = hwm[t] - equity_curve.iloc[t]
        duration.iloc[t] = 0 if drawdown.iloc[t] == 0 else duration.iloc[t-1] + 1

    return drawdown.max(), duration.max()
