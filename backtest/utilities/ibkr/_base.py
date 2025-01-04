import threading
import time

from decimal import Decimal
from sqlalchemy import text

from ibapi.client import *
from ibapi.wrapper import *
from ibapi.contract import Contract
from ibapi.order import *
from ibapi.utils import floatMaxString, decimalMaxString

from backtest.utilities.utils import get_db_engine


class IBClient(EWrapper, EClient):
    def __init__(self, live=False):
        EClient.__init__(self, self)
        self.fill_dict = {}
        self.eng = get_db_engine()
        self.live = live

        # hardcoded port to 7497, change to 7496 when going live
        self.connect("127.0.0.1", 7496 if self.live else 7497, clientId=9999)
        self.create_tws_connection()
        self.symbols = []
        self.portfolio_detail = dict()

    def create_tws_connection(self) -> None:
        def run_loop():
            self.run()

        self.api_thread = threading.Thread(target=run_loop, daemon=True)
        self.api_thread.start()
        time.sleep(1)  # to allow connection to server

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        ''' eg raw output
        12111 265598,AAPL,STK,,0,,,SMART,NASDAQ,USD,AAPL,NMS,False,,,,combo:,NMS,0.01,ACTIVETIM,AD,ADDONT,ADJUST,ALERT,ALGO,ALLOC,AON,AVGCOST,BASKET,BENCHPX,CASHQTY,COND,CONDORDER,DARKONLY,DARKPOLL,DAY,DEACT,DEACTDIS,DEACTEOD,DIS,DUR,GAT,GTC,GTD,GTT,HID,IBKRATS,ICE,IMB,IOC,LIT,LMT,LOC,MIDPX,MIT,MKT,MOC,MTL,NGCOMB,NODARK,NONALGO,OCA,OPG,OPGREROUT,PEGBENCH,PEGMID,POSTATS,POSTONLY,PREOPGRTH,PRICECHK,REL,REL2MID,RELPCTOFS,RPI,RTH,SCALE,SCALEODD,SCALERST,SIZECHK,SMARTSTG,SNAPMID,SNAPMKT,SNAPREL,STP,STPLMT,SWEEP,TRAIL,TRAILLIT,TRAILLMT,TRAILMIT,WHATIF,SMART,AMEX,NYSE,CBOE,PHLX,ISE,CHX,ARCA,NASDAQ,DRCTEDGE,BEX,BATS,EDGEA,BYX,IEX,EDGX,FOXRIVER,PEARL,NYSENAT,LTSE,MEMX,IBEOS,OVERNIGHT,TPLUS0,PSX,1,0,APPLE INC,,Technology,Computers,Computers,US/Eastern,20241126:0400-20241126:2000;20241127:0400-20241127:2000;20241128:CLOSED;20241129:0400-20241129:1700,20241126:0930-20241126:1600;20241127:0930-20241127:1600;20241128:CLOSED;20241129:0930-20241129:1300,,0,,,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,26,1,[133278011824400: ISIN=US0378331005;],,COMMON,,,,,,False,False,0,False,,,,,False,,0.0001,0.0001,100
        '''
        details_dict = {}
        details_dict['symbol'] = contractDetails.contract.symbol
        details_dict['secType'] = contractDetails.contract.secType
        details_dict['primaryExchange'] = contractDetails.contract.primaryExchange
        details_dict['industry'] = contractDetails.industry
        details_dict['category'] = contractDetails.category
        details_dict['subcategory'] = contractDetails.subcategory
        print(reqId, details_dict)
        # save to db
        insert_sql = f""" INSERT INTO ibkr.symbol_info (symbol, secType, primaryExchange, industry, category, subCategory)
            VALUES ('{contractDetails.contract.symbol}', '{contractDetails.contract.secType}', 
                    '{contractDetails.contract.primaryExchange}', '{contractDetails.industry}',
                    '{contractDetails.category}', '{contractDetails.subcategory}')
        """
        insert_sql = text(insert_sql)
        with self.eng.connect() as conn:
            conn.execute(insert_sql)
            conn.commit()

    def contractDetailsEnd(self, reqId: int):
        print("ContractDetailsEnd. ReqId:", reqId)

    def clear_symbols(self):
        self.symbols.clear()

    ## Account Summary ##
    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        print("AccountSummary. ReqId:", reqId, "Account:", account,
              "Tag: ", tag, "Value:", value, "Currency:", currency)

    def accountSummaryEnd(self, reqId: int):
        print("AccountSummaryEnd. ReqId:", reqId)
    
    def updateAccountValue(self, key: str, val: str, currency: str, accountName: str):
        print("UpdateAccountValue. Key:", key, "Value:", val,
              "Currency:", currency, "AccountName:", accountName, '\n')
        if key == "TotalCashBalance" and currency == "BASE":
            self.portfolio_detail['cash'] = val
        elif key == 'NetLiquidationByCurrency' and currency == 'BASE':
            self.portfolio_detail['total'] = val

    def updatePortfolio(self,
                        contract: Contract,
                        position: Decimal,
                        marketPrice: float,
                        marketValue: float,
                        averageCost: float,
                        unrealizedPNL: float,
                        realizedPNL: float,
                        accountName: str):
        inst_portfolio_detail = dict(
            # Symbol=contract.symbol,
            SecType=contract.secType,
            Exchange=contract.exchange,
            Position=decimalMaxString(position),
            MarketPrice=floatMaxString(marketPrice),
            MarketValue=floatMaxString(marketValue),
            AverageCost=floatMaxString(averageCost),
            UnrealizedPNL=floatMaxString(unrealizedPNL),
            RealizedPNL=floatMaxString(realizedPNL),
            AccountName=accountName,
        )
        sym = contract.symbol
        if contract.secType == 'OPT':
            sym = f'{sym}_{contract.right}{contract.strike}_{contract.lastTradeDateOrContractMonth}'
        elif contract.secType == 'BOND':
            # TODO: Implement
            pass
        print(sym, inst_portfolio_detail)
        self.portfolio_detail[sym] = inst_portfolio_detail

    def updateAccountTime(self, timeStamp: str):
        # self.portfolio_detail['timestamp'] = timeStamp
        print("UpdateAccountTime. Time:", timeStamp)

    def accountDownloadEnd(self, accountName: str):
        print("AccountDownloadEnd. Account:", accountName)
