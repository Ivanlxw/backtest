from ibapi.client import *
from ibapi.wrapper import *
from ibapi.contract import Contract
import threading
import time

class IBMktData(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        
    def historicalData(self, reqId, bar):
        print("HistoricalData. ReqId:", reqId, "BarData.", bar)
    
    def historicalSchedule(self, reqId: int, startDateTime: str, endDateTime: str, timeZone: str, sessions: ListOfHistoricalSessions):
        print("HistoricalSchedule. ReqId:", reqId, "Start:", startDateTime, "End:", endDateTime, "TimeZone:", timeZone)
        for session in sessions:
            print("\tSession. Start:", session.startDateTime, "End:", session.endDateTime, "Ref Date:", session.refDate)
    
    def historicalDataUpdate(self, reqId: int, bar):
        print("HistoricalDataUpdate. ReqId:", reqId, "BarData.", bar)
    
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)

    def contractDetails(self, reqId: int, contractDetails):
        print(reqId, contractDetails)

    def contractDetailsEnd(self, reqId: int):
        print("ContractDetailsEnd. ReqId:", reqId)

    def historicalTicks(self, reqId: int, ticks, done: bool):
        print("historicalTicks")
        for tick in ticks:
            print("historicalTicks. ReqId:", reqId, tick)

    def historicalTicksLast(self, reqId: int, ticks, done: bool):
        print("historicalTicksLast")
        for tick in ticks:
            print("HistoricalTickLast. ReqId:", reqId, tick)

    def historicalTicksBidAsk(self, reqId: int, ticks, done: bool):
        print("historicalTicksBidAsk")
        for tick in ticks:
            print("historicalTicksBidAsk. ReqId:", reqId, tick)
        
def websocket_con():
    app.run()
    
if __name__ == "__main__":
    app = IBMktData()      
    app.connect("127.0.0.1", 7497, clientId=1)
    
    con_thread = threading.Thread(target=websocket_con, daemon=True)
    con_thread.start()
    
    contract = Contract()
    contract.symbol = "RWM"
    contract.secType = "STK"
    # contract.secType = "IND"
    # contract.secIdType = "ISIN"
    # contract.SecId = "US74347G1351"
    contract.exchange = "SMART"
    contract.primaryExchange = "ARCA"
    contract.currency = "USD"
    app.reqContractDetails(2004, contract)
    
    app.reqHistoricalData(reqId=2101, 
                        contract=contract,
                        endDateTime='20241101 11:38:33 US/Eastern', 
                        durationStr='5 D',
                        barSizeSetting='1 hour',
                        whatToShow='TRADES',
                        useRTH=0,                 #0 = Includes data outside of RTH | 1 = RTH data only 
                        formatDate=2,    
                        keepUpToDate=0,           #0 = False | 1 = True 
                        chartOptions=[])