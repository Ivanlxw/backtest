import os
from typing import List
import requests

import pandas as pd
from polygon import RESTClient

from Data.source.base.DataGetter import DataGetter


class Polygon(DataGetter):
    def __init__(self, inst_type: str) -> None:
        super().__init__(inst_type)
        self._api_key = os.environ["POLYGON_API"]
        self.client = RESTClient(self._api_key)
        self.BASE_URL = "https://api.polygon.io/"
        self._data_getter_options = ["csv", "gz", "proto"]
        self._80_days_in_ms = 6912000000
        self._limit_count = 49500

    def _add_api_to_url(self, url):
        return url + f"&apiKey={self._api_key}"

    def equity_chop_dates(self, from_ms, to_ms, freq) -> List[tuple]:
        if freq == "day":
            return [(from_ms, to_ms)]
        tuple_ms = []
        start_idx = from_ms
        while start_idx + self._80_days_in_ms < to_ms:
            tuple_ms.append((start_idx, start_idx + self._80_days_in_ms))
            start_idx += self._80_days_in_ms
        tuple_ms.append((start_idx, to_ms))
        return tuple_ms


    def get_ohlc_internal_depreciated(self, symbol, multiplier, freq, from_ms, to_ms) -> list:
        """Reads using request and write raw data in provided compression"""
        url = (
            self.BASE_URL
            + f"v2/aggs/ticker/{symbol}/range/{multiplier}/{freq}/{from_ms}/{to_ms}"
            + f"?adjusted=true&sort=asc&limit={self._limit_count}"
        )
        url = self._add_api_to_url(url)
        resp = requests.get(url, timeout=60)
        resp_json = resp.json()
        if not resp.ok:
            raise Exception(f"Failed to query polygon ohlc. url={url},resp={resp}")
        elif resp_json["queryCount"] == self._limit_count or resp_json["resultsCount"] == self._limit_count:
            raise Exception(
                f"Need to chop dates into finer pieces: queryCount={resp_json['queryCount']},resultsCount={resp_json['resultsCount']}"
            )
        if "results" not in resp_json.keys():
            return []
        return resp_json["results"]
    
    def get_ohlc(self, symbol, multiplier, freq, from_ms, to_ms) -> pd.DataFrame:
        try:
            results = [a for a in self.client.list_aggs(symbol, multiplier, freq, from_ms, to_ms, limit=50000)]    
        except KeyError:
            return pd.DataFrame()
        results = pd.DataFrame(results)
        if not results.empty:
            results.rename({
                'transactions': 'num_trades'
            }, axis=1, inplace=True)
            results.drop("otc", axis=1, inplace=True)
        return results

    def get_option_info(self, underlying_symbol, from_ms, to_ms, expired: bool = True):
        def _chop_finer_dates(from_ms, to_ms) -> List[tuple]:
            tuple_ms = []
            start_idx = from_ms
            while start_idx + self._80_days_in_ms / 2 < to_ms:
                tuple_ms.append((start_idx, start_idx + self._80_days_in_ms / 2))
                start_idx += self._80_days_in_ms / 2
            tuple_ms.append((start_idx, to_ms))
            return tuple_ms

        dates_to_query = _chop_finer_dates(from_ms, to_ms)
        results = []
        for start_ms, end_ms in dates_to_query:
            start_ms_str = pd.Timestamp(start_ms, unit="ms").strftime("%Y-%m-%d")
            end_ms_str = pd.Timestamp(end_ms, unit="ms").strftime("%Y-%m-%d")
            url = (
                self.BASE_URL
                + f"v3/reference/options/contracts?underlying_ticker={underlying_symbol}&limit=950&expired={str(expired).lower()}"
                f"&expiration_date.gte={start_ms_str}&expiration_date.lt={end_ms_str}"
            )
            url = self._add_api_to_url(url)
            resp = requests.get(url, timeout=45)
            if not resp.ok:
                raise Exception(f"Failed to query polygon option info. url={url},resp={resp.text}")
            resp_json = resp.json()
            results.extend(resp_json["results"])
        return results


def get_source_instance(inst_type):
    return Polygon(inst_type)
