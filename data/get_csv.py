import requests
import pandas as pd
import os
from datetime import datetime

def getData_csv(symbol, csv_dir, full=False, interval=None,):
    print(f"Getting symbol: {symbol}")

    key = "GOTIYRMA96Y0WN5U"
    if interval != None:
        if interval not in ['1min', '5min', '15min', '30min', '60min']:
            raise Exception("internval has to be one of the following options (string):\n'1min', '5min', '15min', '30min', '60min'")

        if full:
            url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=" + symbol + "&interval=" + interval +"&outputsize=full" + "&apikey=" + key
        else: 
            url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=" + symbol + "&interval=" + interval + "&apikey=" + key            
        
        if not os.path.exists(f"{interval}"):
            os.mkdir(f"{interval}")

        filepath = f"./{interval}/{symbol}.csv"
    else:
        if full:
            url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=" + symbol + "&outputsize=full" + "&apikey=" + key
        else: 
            url = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=" + symbol + "&apikey=" + key
        
        if not os.path.exists(csv_dir):
            os.mkdir(csv_dir)

        filepath = csv_dir + "/{}.csv".format(symbol)

    parsed_data = requests.get(url).json()
    df = pd.DataFrame.from_dict(parsed_data['Time Series (Daily)'], orient='index')
    df = df.iloc[::-1]  ## reverse from start to end instead of end to start

    merge_n_save(filepath, df)

    

def get_tiingo_eod(ticker, fp, full:bool):
    TIINGO_TOKEN = "8333c1fb24c9f861e940450cbd8b7f88691ecc3d"
    headers = {
            'Content-Type': 'application/json'
    }
    if full:
            start_date = "2000-1-1"
            end_date = "2020-1-1"
    else:
            start_date = "2020-1-1"
            end_date = datetime.today().strftime('%Y-%m-%d')

    url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={start_date}&endDate={end_date}&resampleFreq=daily&token={TIINGO_TOKEN}"

    requestResponse = requests.get(url, headers=headers)
    json_df = requestResponse.json()
    df = pd.DataFrame(json_df)
    df['date'] = df['date'].apply(lambda x: x.replace("T00:00:00.000Z", ""))
    df.index = df['date']
    df = df.drop("date", axis=1)
    df = df[["open", "high", "low", "close", "volume"]]
    
    merge_n_save(fp, df)

def merge_n_save(filepath, df):
    daily_fp =os.path.join(os.getcwd(), 'data','data', 'daily')
    if not os.path.exists(daily_fp):
        os.makedirs(daily_fp)
    if os.path.exists(filepath):
        ## merge data
        existing_df = pd.read_csv(filepath, index_col=0)
        df = pd.concat([existing_df, df])
        df = df.drop_duplicates()

    df.to_csv(filepath)
    print("Data is stored as csv!")
