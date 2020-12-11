import os, sys
# sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  ## 2 dirs above

from abc import abstractmethod, ABC
import pandas as pd

class StatisticalData(ABC):
    @abstractmethod
    def preprocess_X(self):
        raise NotImplementedError("Should implement preprocess_X()")

    @abstractmethod 
    def _transform_X(self, df: pd.DataFrame):
        raise NotImplementedError("_transform_X() not implemented. \
            return df if no transformation.")

    @abstractmethod
    def preprocess_Y(self):
        raise NotImplementedError("Should implement preprocess_Y()")

    @abstractmethod
    def process_data(self) -> dict:
        raise NotImplementedError("process_data() must be implemented.\n \
            Should return dict(pd.DataFrame) where keys are symbol name.")


class BaseStatisticalData(StatisticalData):
    def __init__(self, bars, shift: int, lag: int=0):
        print("Basic Data Model for supervised models")
        ##  one whole dataframe concatnated in a dict
        self.lag = lag
        if lag < 0:
            raise Exception("self.lag variable should not be negative")
        elif lag != 0:
            self.lag = lag + 1

        if shift > 0:
            self.shift = -shift
        else:
            self.shift = shift

    def get_shift(self):
        return abs(self.shift)

    # returns dict(pd.DataFrame)
    def preprocess_X(self, df:pd.DataFrame):
        return self._transform_X(df)

    def _transform_X(self, df: pd.DataFrame): 
        ## obtains lagged data for lag days
        if self.lag > 0:
            for i in range(1,self.lag):
                df.loc[:, "lag_"+str(i)] = df["Close"].shift(-i)
        return df.dropna()
    
    def preprocess_Y(self, X):
        ## derive Y from transformed X
        ## In this basic example, our reference is the price self.shift days from now.
        X.loc[:, "target"] = X["Close"].shift(self.shift)
    
    # must be implemented
    ## appends all data into 1 large dataframe with extra col - ticker
    ## returns (pd.DataFrame, pd.Series)
    def process_data(self, data):
        X = self.preprocess_X(data)
        self.preprocess_Y(X)
        final_data = X.dropna()
        return final_data.drop('target', axis=1), final_data['target']

class ClassificationData(BaseStatisticalData):
    def __init__(self, bars, shift, lag:int=0, perc_change:float=0.05):
        super().__init__(bars, shift)
        self.lag = lag
        self.perc_chg = perc_change
        if self.lag != 0:
            self.lag = lag + 1
    
    def to_buy_or_sell(self, perc):
        if perc > self.perc_chg:
            return 1
        elif perc < 1-self.perc_chg:
            return -1
        else:
            return 0

    def preprocess_Y(self, X):
        X.loc[:, "target_num"] = X["Close"].shift(self.shift)
        X.loc[:, "target"] = (X["target_num"] / X["Close"])
        X["target"] = X["target"].apply(self.to_buy_or_sell)
        del X["target_num"]
