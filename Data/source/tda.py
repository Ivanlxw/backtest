import argparse
import logging
import os
from pathlib import Path
import urllib

import pandas as pd
import requests
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By

from Data.source.base.DataGetter import DataGetter
from backtest.utilities.utils import DATA_GETTER_INST_TYPES, load_credentials, read_universe_list


class TDAData(DataGetter):
    def __init__(self, universe_list, inst_type: str) -> None:
        super().__init__(universe_list, inst_type)
        self.consumer_key = os.environ["TDD_consumer_key"]
        self.account_id = os.environ["TDA_account_id"]
        self.access_token = None
        self.refresh_token = None
        self.BASE_URL = "https://api.tdameritrade.com/v1"
        self.get_token("authorization")

    def _signin_code(self):
        driver = webdriver.Firefox()
        url = f"https://auth.tdameritrade.com/auth?response_type=code&redirect_uri=http://localhost&client_id={self.consumer_key}@AMER.OAUTHAP"
        driver.get(url)

        userId = driver.find_element(By.CSS_SELECTOR, "#username0")
        userId.clear()
        userId.send_keys(os.environ["TDA_username"])
        pw = driver.find_element(By.CSS_SELECTOR, "#password1")
        pw.clear()
        pw.send_keys(f"{os.environ['TDA_pw']}")
        login_button = driver.find_element(By.CSS_SELECTOR, "#accept")
        login_button.click()

        # click accept
        accept_button = driver.find_element(By.CSS_SELECTOR, "#accept")
        try:
            accept_button.click()
        except selenium.common.exceptions.WebDriverException:
            new_url = driver.current_url
            code = new_url.split("code=")[1]
            logging.info("Coded:\n"+code)
            return code
        finally:
            driver.close()

    def get_token(self, grant_type):
        post_base_url = r"https://api.tdameritrade.com/v1/oauth2/token"
        if grant_type == "authorization":
            code = self._signin_code()
            if code is not None:
                code = urllib.parse.unquote(code)
                logging.info("Decoded:\n"+code)
                params = {
                    "grant_type": "authorization_code",
                    "access_type": "offline",
                    "code": code,
                    "client_id": self.consumer_key,
                    "redirect_uri": "http://localhost",
                }
                headers = {"Content-Type": "application/x-www-form-urlencoded"}
                res = requests.post(
                    post_base_url, headers=headers, data=params)
                if res.ok:
                    res_body = res.json()
                    logging.info("Obtained access_token & refresh_token")
                    self.access_token = res_body["access_token"]
                    self.refresh_token = res_body["refresh_token"]
                else:
                    print(res)
                    print(res.json())
                    raise Exception(
                        f"API POST exception: Error {res.status_code}")
            else:
                raise Exception("Could not sign in and obtain code")
        elif grant_type == "refresh":
            res = requests.post(post_base_url, data={
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": self.consumer_key,
            })
            if res.ok:
                res_body = res.json()
                self.access_token = res_body["access_token"]
                print(res_body["access_token"])
            else:
                print(res.json())

    def get_account_details(self):
        return requests.get(
            f"{self.BASE_URL}/accounts/{os.environ['TDA_account_id']}",
            headers={"Authorization": f"Bearer {self.access_token}"},
        ).json()

    def get_ohlc(self, symbol, multiplier, freq, from_ms, to_ms):
        # get period_type and period from from_ms
        timedelta = pd.Timedelta(to_ms - from_ms, unit='ms')
        period_type = None
        if timedelta.days < 365:
            period_type = 'days'
            period = timedelta.days
        else:
            period_type = 'year'
            period = (timedelta.days // 365) + 1
        res = requests.get(
            f"https://api.tdameritrade.com/v1/marketdata/{symbol}/pricehistory?period={period}&periodType={period_type}&frequencyType={freq}&frequency={multiplier}&endDate={to_ms}",
            headers={"Authorization": f"Bearer {tda_getter.access_token}"},
        )
        if res.ok:
            res = res.json()
        return res


def parse_args():
    parser = argparse.ArgumentParser(
        description="Get ticker csv data via API calls to either AlphaVantage or Tiingo."
    )
    parser.add_argument("-c", "--credentials", required=True,
                        type=str, help="filepath to credentials.json",)
    parser.add_argument("--universe", type=Path,  required=True,
                        help="File path to trading universe", nargs="+")
    parser.add_argument("--inst-type", type=str, required=False,
                        default='equity', choices=DATA_GETTER_INST_TYPES)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    load_credentials(args.credentials, into_env=True)
    universe_list = read_universe_list(args.universe)
    tda_getter = TDAData(universe_list, args.inst_type)
