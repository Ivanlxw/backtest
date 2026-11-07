from ibapi.client import *
from ibapi.wrapper import *


class TestApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)

    def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
        print(f"openOrder. orderId: {orderId}, contract: {contract}, order: {order}")

    def orderStatus(self, orderId: OrderId, status: str, filled: Decimal, remaining: Decimal, avgFillPrice: float, 
                    permId: int, parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
        print(f'''
            orderId: {orderId}, status: {status}
            filled: {filled}, remaining: {remaining}
            avgFillPrice: {avgFillPrice}, lastFillPrice: {lastFillPrice}
            whyHeld: {whyHeld}, mktCapPrice: {mktCapPrice}

            permId: {permId}, parentId: {parentId}, clientId: {clientId}, 
        ''')

    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        print(f"reqId: {reqId}, contract: {contract}, execution: {execution}")
    
    
    def nextValidId(self, orderId: OrderId):
        mycontract = Contract()
        mycontract.symbol = "COIN"
        mycontract.secType = "STK"
        mycontract.exchange = "SMART"
        mycontract.currency = "USD"
        self.reqContractDetails(orderId, mycontract)

    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        print(contractDetails.contract)
        myorder = Order()
        myorder.orderId = reqId
        myorder.action = "BUY"
        myorder.tif = "DAY"
        myorder.orderType = "LMT"
        myorder.lmtPrice = 200.80
        myorder.totalQuantity = 10
        self.placeOrder(myorder.orderId, contractDetails.contract, myorder)


if __name__ == "__main__":
    app = TestApp()
    app.connect("127.0.0.1", 7497, 100)
    app.run()
