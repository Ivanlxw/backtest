import os
import time
from typing import List
import requests

import pandas as pd

from Data.source.base.DataGetter import DataGetter


class Polygon(DataGetter):
    def __init__(self, universe_list, inst_type: str) -> None:
        super().__init__(universe_list, inst_type)
        self.BASE_URL = "https://api.polygon.io/"
        self._data_getter_options = ["csv", "gz", "proto"]
        self._api_key = os.environ["POLYGON_API" if inst_type  == "equity" else "POLYGON_OPTION_API"]
        self._sixty_days_in_ms = 5184000000
        self._limit_count = 49500

    def _add_api_to_url(self, url):
        return url + f"&apiKey={self._api_key}"

    def _chop_dates(self, from_ms, to_ms, freq) -> List[tuple]:
        if freq == "day":
            return[(from_ms, to_ms)]
        tuple_ms = []
        start_idx = from_ms
        while start_idx + self._sixty_days_in_ms < to_ms:
            tuple_ms.append((start_idx, start_idx + self._sixty_days_in_ms))
            start_idx += self._sixty_days_in_ms
        tuple_ms.append((start_idx, to_ms))
        return tuple_ms

    def get_ohlc_internal(self, symbol, multiplier, freq, from_ms, to_ms) -> list:
        """Reads using request and write raw data in provided compression"""
        url = (
            self.BASE_URL
            + f"v2/aggs/ticker/{symbol}/range/{multiplier}/{freq}/{from_ms}/{to_ms}"
            + f"?adjusted=true&sort=asc&limit={self._limit_count}"
        )
        url = self._add_api_to_url(url)
        resp = requests.get(url, timeout=45)
        resp_json = resp.json()
        if not resp.ok:
            raise Exception(
                f"Failed to query polygon ohlc. url={url},resp={resp}")
        elif (
            resp_json["queryCount"] == self._limit_count
            or resp_json["resultsCount"] == self._limit_count
        ):
            raise Exception(
                f"Need to chop dates into finer pieces: queryCount={resp_json['queryCount']},resultsCount={resp_json['resultsCount']}"
            )
        if "results" not in resp_json.keys():
            return []
        return resp_json["results"]

    def get_ohlc(self, symbol, multiplier, freq, from_ms, to_ms) -> pd.DataFrame:
        dates_to_query = self._chop_dates(from_ms, to_ms, freq)
        results = []
        for start_ms, end_ms in dates_to_query:
            results.extend(self.get_ohlc_internal(
                symbol, multiplier, freq, start_ms, end_ms))
        results = pd.DataFrame(results)
        if not results.empty:
            results.set_index("t", inplace=True)
        return results

    def _get_option_info_internal(self, underlying_symbol, from_ms, to_ms):
        dates_to_query = self._chop_dates(from_ms, to_ms)
        results = []
        idx = 0

        for start_ms, end_ms in dates_to_query:
            if idx % 5 == 2:
                time.sleep(60)
            start_ms_str = pd.Timestamp(
                start_ms, unit="ms").strftime("%Y-%m-%d")
            end_ms_str = pd.Timestamp(end_ms, unit="ms").strftime("%Y-%m-%d")
            url = (
                self.BASE_URL
                + f"v3/reference/options/contracts?underlying_ticker={underlying_symbol}&limit=500&expired=true&expiration_date.gte={start_ms_str}&expiration_date.lt={end_ms_str}"
            )
            url = self._add_api_to_url(url)
            resp = requests.get(url, timeout=45)
            if not resp.ok:
                raise Exception(
                    f"Failed to query polygon option info. url={url},resp={resp.text}")
            resp_json = resp.json()
            results.extend(resp_json["results"])
        return results

    def get_option_info(self, from_ms, to_ms):
        now_ms = time.time() * 1000
        if to_ms > now_ms:
            raise Exception(
                "Polygon::get_option_info() currently does not support querying for future expiries")
        res = [contract_info for underlying in self.universe_list for contract_info in self._get_option_info_internal(
            underlying, from_ms, to_ms)]
        return res


def get_source_instance(universe_fp, inst_type):
    return Polygon(universe_fp, inst_type)
