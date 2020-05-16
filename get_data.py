import requests

headers = {
        'Content-Type': 'application/json'
}
ticker = "AAPL"
url = f"https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate=2010-1-1&endDate=2016-1-1&format=csv&resampleFreq=daily"

requestResponse = requests.get(url, headers=headers)
print(requestResponse.json())