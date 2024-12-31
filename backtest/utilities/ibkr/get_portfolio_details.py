import argparse
import os
import time

from sqlalchemy import create_engine
from ibapi.account_summary_tags import AccountSummaryTags

from backtest.utilities.ibkr._base import IBClient

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process a space-delimited list of strings.")
    parser.add_argument(
        '-s', '--symbol',
        nargs='+',
        required=False,
        default=None,
        help='A space-delimited list of symbols',
    )
    parser.add_argument(
        '-f', '--filepath',
        required=False,
        default=None,
        help="A txt file containing a list of symbols to query"
    )
    args = parser.parse_args()

    eng = create_engine(os.environ["DB_URL"])
    c = IBClient(eng, live=True)
    acct_summary_req_id = 10003
    # c.reqAccountSummary(acct_summary_req_id, "All", AccountSummaryTags.AllTags)
    # c.cancelAccountSummary(acct_summary_req_id)
    
    acc_id = os.environ['IBKR_PAPER_USERID']
    c.reqAccountUpdates(True, acc_id)
    time.sleep(4)
    print(c.portfolio_detail)
    c.reqAccountUpdates(False, acc_id)
