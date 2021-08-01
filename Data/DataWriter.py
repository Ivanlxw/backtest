"""
  A script to update data daily. Especially useful for intraday data as history is short and we should save data
  Updates using TDA API since we are eventually trading with TDA
"""
import os
import time
import logging
import requests
import concurrent.futures as fut
import numpy as np
import pandas as pd
from pathlib import Path
from backtest.utilities.utils import load_credentials, log_message, parse_args, remove_bs

# TODO: use direnv
args = parse_args()
load_credentials(args.credentials)

ABSOLUTE_FILEDIR = Path(os.path.dirname(os.path.abspath(__file__)))
logging.basicConfig(filename=ABSOLUTE_FILEDIR /
                    'logging/DataWriter.log', level=logging.INFO)
with open(ABSOLUTE_FILEDIR / "us_stocks.txt") as fin:
    SYM_LIST = list(map(remove_bs, fin.readlines()))
NY = 'America/New_York'
TIMEFRAMES = ["daily", "1min", "5min", "10min", "15min", "30min"]

def get_price_history(ticker, period_type: str, period: int, frequency_type: str, frequency: int):
    assert frequency_type in ["minute", "daily", "weekly", "monthly"]
    assert frequency in [1, 5, 10, 15, 30]
    try:
        res = requests.get(
            f"https://api.tdameritrade.com/v1/marketdata/{ticker}/pricehistory",
            params={
                "apikey": os.environ["TDD_consumer_key"],
                "periodType": period_type,
                "period": period,
                "frequencyType": frequency_type,
                "frequency": frequency,
            },
        )
    except requests.exceptions.RequestException as e:
        log_message(f"[get_price_history] Cannot get {ticker}")
        return None
    if res.ok:
        return res.json()


def _call_fmp_api(url: str):
    resp = requests.get(url)
    if resp.ok:
        data = pd.DataFrame(resp.json()).iloc[::-1]
        if data.empty:
            raise Exception("dataframe empty")
        # sometimes therre are empty values
        data.replace('', np.nan, inplace=True)
        data.dropna(inplace=True)
        data.set_index("date", inplace=True)
        # TODO: Sometimes data comes with empty index - a whitespace
        data.index = data.index.map(pd.to_datetime)
        return data


def _get_historical_dcf(sym: str):
    url = f"https://financialmodelingprep.com/api/v3/historical-daily-discounted-cash-flow/{sym}?period=quarterr&apikey={os.environ['FMP_API']}"
    data = _call_fmp_api(url)
    return data.reindex(pd.date_range(
        data.index[0], data.index[-1], freq='d'), method="pad")


def _get_historical_financial_growth(sym: str):
    url = f"https://financialmodelingprep.com/api/v3/financial-growth/{sym}?period=quarter&apikey={os.environ['FMP_API']}"
    return _call_fmp_api(url)


def _get_historical_financial_ratios(sym: str):
    url = f"https://financialmodelingprep.com/api/v3/ratios/{sym}?period=quarter&limit=140&apikey={os.environ['FMP_API']}"
    return _call_fmp_api(url)


def _get_company_key_metrics(sym: str):
    url = f"https://financialmodelingprep.com/api/v3/key-metrics/{sym}?period=quarter&limit=130&apikey={os.environ['FMP_API']}"
    return _call_fmp_api(url)


def get_fundamentals_hist_quarterly(ticker: str):
    dfs = []
    dfs.append(_get_historical_dcf(ticker))
    dfs.append(_get_historical_financial_growth(ticker))
    dfs.append(_get_historical_financial_ratios(ticker))
    dfs.append(_get_company_key_metrics(ticker))
    return pd.concat(dfs, axis=1, join="inner")


def update_data(time_freq: str, symbol_list: list):
    csv_dir = ABSOLUTE_FILEDIR / f"data/{time_freq}"
    if not os.path.exists(csv_dir):
        raise Exception(
            f"File for frequency ({time_freq}) does not exist")
    if time_freq == "daily":
        period_type = "month"
        period = 1
        frequency_type = "daily"
        freq = 1
    else:
        assert time_freq.lower().endswith("min")
        period_type = "day"
        period = 10
        frequency_type = "minute"
        freq = int(time_freq[:-3])

    missing_syms = []
    for idx, symbol in enumerate(symbol_list):
        if idx % 20 == 0:
            log_message(f"updating symbol idx: {idx}")
        df = get_price_history(symbol, period_type,
                               period, frequency_type, freq)  # json format
        if df is not None and "candles" in df.keys() and not df["empty"]:
            data = pd.DataFrame(df["candles"])
            data.set_index("datetime", inplace=True)
            # need to convert time of data to 12am -- only for daily
            if time_freq == "daily":
                data.index = data.index.map(lambda x: pd.Timestamp(
                    x, unit="ms").normalize().value // 10 ** 6)
            csv_fp = csv_dir / f"{symbol}.csv"
            if os.path.exists(csv_fp):
                existing = pd.read_csv(csv_fp, index_col=0)
                data = pd.concat(
                    [existing, data], axis=0)
                data = data[~data.index.duplicated(keep='last')]
            data.to_csv(csv_fp)
        else:
            missing_syms.append(symbol)
    log_message(f"[{time_freq}] missing symbols: {missing_syms}, trying again")
    if len(missing_syms) > 20: 
        update_data(time_freq, missing_syms)


def update_ohlc(time_freq: str, symbol_list:list):
    assert time_freq in TIMEFRAMES
    update_data(time_freq, symbol_list)
    log_message(f"update_data done for timeframe {time_freq}")
    time.sleep(60)


def main():
    while True:
        now = pd.Timestamp.now(tz=NY)
        if not (now.hour == 0 and now.minute == 15 and now.dayofweek > 0):
            continue
        log_message("Update data")
        processes = []
        with fut.ProcessPoolExecutor(5) as p:
            for time_freq in TIMEFRAMES:
                processes.append(p.submit(update_ohlc, time_freq, SYM_LIST))
        processes = [t.result() for t in processes]
        if now.dayofweek >= 5:
            break
        log_message("sleeping")
        time.sleep(18 * 3600)  # 18 hrs
        log_message("sleep over")

def update_ohlc_once(symbol_list):
    processes = []
    with fut.ProcessPoolExecutor(5) as p:
        for time_freq in TIMEFRAMES:
            processes.append(p.submit(update_ohlc, time_freq, symbol_list))
    processes = [t.result() for t in processes]

def write_fundamental():
    fundamental_dir = ABSOLUTE_FILEDIR / "data/fundamental/quarterly"
    failed_syms = []
    for sym in SYM_LIST:
        try:
            data = get_fundamentals_hist_quarterly(sym)
            # remove duplicate cols
            data = data.loc[:, ~data.columns.duplicated()]
            csv_fp = fundamental_dir / f"{sym}.csv"
            if os.path.exists(csv_fp):
                existing = pd.read_csv(csv_fp, index_col=0, header=0)
                if not existing.empty:
                    existing.index = existing.index.map(pd.to_datetime)
                    data = pd.concat([existing, data])
                    data = data[~data.index.duplicated(keep='last')]
                    data.dropna(axis=1, how='all', inplace=True)
                    data.sort_index(inplace=True)
            if not data.empty:
                data.to_csv(csv_fp)
            else:
                failed_syms.append(sym)
        except Exception as e:
            log_message(f"[write_fundamental] {sym} failed with : {e}")
            failed_syms.append(sym)
    log_message(f"[write_fundamental] Failed syms - {failed_syms}")
    with open("failed_syms.txt", "w") as fout:
        fout.write("\n".join(failed_syms))


if __name__ == "__main__":
    main()
    # update_ohlc_once(['TMUS', 'TXN', 'QCOM', 'SBUX', 'AMGN', 'MRNA', 'AMAT', 'AMD', 'ZM', 'MDLZ', 'LRCX', 'ABNB', 'ADP', 'GILD', 'MU', 'FISV', 'CSX', 'ATVI', 'ROKU', 'ADI', 'DOCU', 'CRWD', 'WDAY', 'VRTX', 'MNST', 'BIIB', 'EBAY', 'KHC', 'MAR', 'EXC', 'MTCH', 'ROST', 'EA', 'PAYX', 'WBA', 'ALXN', 'CDNS', 'ETSY', 'FITB', 'EXPE', 'CERN', 'RPRX', 'SPLK', 'ENPH', 'DLTR', 'DISH', 'ZI', 'EXPD', 'HBAN', 'FOXA', 'DKNG', 'TER', 'TTWO', 'CZR', 'AKAM', 'WDC', 'EXAS', 'HOLX', 'LYFT', 'NTAP', 'PFG', 'NUAN', 'INCY', 'VTRS', 'BSY', 'CG', 'AFRM', 'PPD', 'COUP', 'UAL', 'ON', 'PLUG', 'LKQ', 'LNT', 'PFPT', 'NTLA', 'NTRA', 'BMBL', 'BLDR', 'ARCC', 'FSLR', 'AGNC', 'ZION', 'CROX', 'GNTX', 'BYND', 'NTNX', 'MAT', 'EGOV', 'UPWK', 'IIVI', 'TCF', 'PBCT', 'CHNG', 'WSC', 'WOOF', 'RDFN', 'ANGI', 'SFIX', 'PACB', 'WISH', 'RCM', 'SLM', 'CAR', 'APPS', 'AMKR', 'NKLA', 'VRM', 'EXPI', 'TRIP', 'EXEL', 'VLY', 'ONEM', 'VIAV', 'NAVI', 'URBN', 'STAY', 'VG', 'ISBC', 'ACAD', 'IOVA', 'OSTK', 'ASO', 'PS', 'PSEC', 'MIK', 'RIOT', 'ALLO', 'BBBY', 'BCRX', 'SFM', 'MARA', 'EDIT', 'CRSR', 'SDC', 'UNIT', 'OPK', 'MDRX', 'GRWG', 'SRNE', 'ALEC', 'CALD', 'FOLD', 'BLMN', 'CYTK', 'SUMO', 'GLUU', 'MVIS', 'AVIR', 'IRWD', 'GNMK', 'CNST', 'GPRO', 'REAL', 'PTEN', 'VLDR', 'AVXL', 'CDEV', 'PAYA', 'TELL', 'CNDT', 'BLNK', 'OCGN', 'WKHS', 'SSYS', 'HRTX', 'CENX', 'FGEN', 'GEVO', 'PGEN', 'SWBI', 'GOGO', 'IDEX', 'IMGN', 'DVAX', 'AGEN', 'MNKD', 'CCXI', 'MGI', 'VXRT', 'HGEN', 'DHC', 'VUZI', 'ISEE', 'ORBC', 'INSG', 'WETF', 'CERS', 'POWW', 'GNUS', 'ATOS', 'CLSK', 'MUDS', 'KIN', 'AKBA', 'NEXT', 'ATNX', 'TXMD', 'GERN', 'SELB', 'PRVB', 'KERX', 'ATHA', 'ALT', 'ATHX', 'LLNW', 'AMTX', 'CLSD', 'SYRS', 'HOFV', 'CTXR', 'NMTR', 'PDSB', 'UONE', 'ABUS', 'NBEV', 'CERC', 'CHMA', 'ENOB', 'CFMS', 'SURF', 'CIDM', 'MBIO', 'SEEL', 'SNCR', 'TNXP', 'KALA', 'MRKR', 'CRBP', 'RESN', 'XSPA', 'VTVT', 'CASI', 'AGTC', 'ARDX', 'WTER', 'XELA', 'TTOO', 'NOVN', 'KOSS', 'VERB', 'EYES', 'AQMS', 'ADMP', 'AYRO', 'GALT', 'CYCN', 'WATT', 'ACRX', 'ASMB', 'IZEA', 'EVFM', 'AREC', 'MARK', 'ABEO', 'AEHR', 'WWR', 'ELOX', 'SRGA', 'HEPA', 'INPX', 'BOXL', 'COCP', 'NURO', 'VISL', 'PHUN', 'AEMD', 'POAI', 'SLNO', 'DARE', 'ECOR', 'PTE', 'PRPO', 'ALNA', 'NRBO', 'LPTH', 'ADXS', 'AEI', 'RSLS', 'FTEK', 'CEMI', 'BBI', 'DFFN', 'CARV', 'ASTC', 'BSQR', 'RGLS', 'IDRA', 'ASRT', 'SEAC', 'BXRX', 'WINT', 'BLIN', 'AZRX', 'WISA', 'PIXY', 'MDIA', 'TLGT', 'MOTS', 'AMST', 'NXTD', 'RIBT', 'SINT', 'MOSY', 'OBLN', 'EVEP', 'WMT', 'UNH', 'MA', 'HD', 'PG', 'DIS', 'BAC', 'NKE', 'XOM', 'KO', 'ORCL', 'PFE', 'CRM', 'LLY', 'VZ', 'ABT', 'ABBV', 'DHR', 'TMO', 'T', 'MRK', 'CVX', 'WFC', 'MCD', 'MS', 'UPS', 'HON', 'PM', 'NEE', 'BMY', 'UNP', 'C', 'BA', 'AXP', 'LOW', 'RTX', 'AMT', 'MO', 'UBER', 'CCI', 'USB', 'TJX', 'BX', 'DUK', 'HCA', 'GM', 'PNC', 'SNOW', 'CI', 'COP', 'FDX', 'TFC', 'BDX', 'DELL', 'MMC', 'COF', 'CL', 'EW', 'SO', 'ICE', 'TWLO', 'NSC', 'BSX', 'WM', 'D', 'DASH', 'EMR', 'GPN', 'F', 'PGR', 'TWTR', 'DG', 'FCX', 'EPD', 'BAX', 'SLB', 'AIG', 'CNC', 'KMI', 'SRE', 'PRU', 'DD', 'ALL', 'SYY', 'OTIS', 'DFS', 'IFF', 'WELL', 'YUM', 'AFL', 'KKR', 'NET', 'HLT', 'GIS', 'MPC', 'CHWY', 'PXD', 'GLW', 'RKT', 'LVS', 'HPQ', 'ZBH', 'IAU', 'DHI', 'LYB', 'PSX', 'VFC', 'ADM', 'EQR', 'PEG', 'MCK', 'CTVA', 'CCL', 'BLL', 'DAL', 'ED', 'WORK', 'ET', 'TSN', 'WY', 'HRL', 'OXY', 'CUK', 'OKE', 'FTV', 'INVH', 'HES', 'IP', 'DTE', 'MKC', 'VTR', 'CLX', 'HIG', 'PPL', 'BKR', 'AEE', 'EIX', 'K', 'AVTR', 'TDOC', 'CHD', 'FE', 'RCL', 'LB', 'PCG', 'IR', 'PEAK', 'DRI', 'BILL', 'ELAN', 'UDR', 'CAG', 'AES', 'XPO', 'OMC', 'ATUS', 'TXT', 'EVRG', 'MAS', 'BEN', 'CNP', 'OSH', 'HWM', 'PHM', 'LUMN', 'IPG', 'APO', 'AMH', 'CPB', 'WRK', 'GME', 'FNF', 'CLR', 'EQH', 'IRM', 'S', 'ASAN', 'CHGG', 'MPW', 'NLY', 'NI', 'STOR', 'TRGP', 'COLD', 'NCLH', 'WU', 'VRT', 'DKS', 'VST', 'SMAR', 'MRO', 'CMA', 'KIM', 'ARMK', 'OHI', 'UAA', 'DNB', 'UA', 'NLSN', 'CIEN', 'JNPR', 'ADT', 'FHN', 'VNO', 'YETI', 'SEE', 'SKX', 'PLAN', 'JEF', 'KNX', 'KSS', 'SLV', 'EQT', 'USFD', 'TPX', 'ALK', 'ORI', 'STWD', 'EDR', 'X', 'HBI', 'COG', 'HTA', 'AZEK', 'HOG', 'PFGC', 'CFX', 'QTS', 'FL', 'ELY', 'USO', 'AEO', 'HUN', 'ESI', 'GPK', 'UNM', 'FSLY', 'ORCC', 'LPX', 'NVTA', 'NOV', 'KBR', 'LMND', 'PSTG', 'NYCB', 'CHK', 'VNT', 'M', 'CC', 'AI', 'MDLA', 'SHLX', 'JWN', 'RVLV', 'FLO', 'SWCH', 'HFC', 'PRSP', 'SIX', 'FUBO', 'BE', 'FNB', 'BOX', 'MTDR', 'ETRN', 'OMI', 'MAC', 'OUT', 'HL', 'CWH', 'MIC', 'PD', 'MGY', 'SWN', 'CIM', 'APLE', 'OFC', 'MUR', 'WBT', 'SITC', 'TMHC', 'EQC', 'DDD', 'HP', 'SAVE', 'EAF', 'LTHM', 'PEB', 'ASB', 'SJI', 'SLQT', 'TROX', 'ENLC', 'KLDX', 'LEAF', 'AT'])
    # write_fundamental()
    # update_ohlc("daily")
