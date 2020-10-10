import os, sys
# sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  ## 2 dirs above

from abc import abstractmethod, ABC
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from scipy import stats

from event import SignalEvent

class ProcessData(ABC):
    @abstractmethod
    def _process_data(self) -> dict:
        raise NotImplementedError("process_data() must be implemented.\n \
            Should return dict(pd.DataFrame) where keys are symbol name.")
    
    def get_processed_data(self):
        return self._process_data()

class SKData(ABC):
    @abstractmethod
    def preprocess_X(self):
        raise NotImplementedError("Should implement preprocess_X()")

    @abstractmethod 
    def transform_X(self, df: pd.DataFrame):
        raise NotImplementedError("transform_X() not implemented. \
            return df if no transformation.")

    @abstractmethod
    def preprocess_Y(self):
        raise NotImplementedError("Should implement preprocess_Y()")

class BaseSkData(ProcessData, SKData):
    def __init__(self, bars, shift: int):
        print("Basic Data Model for supervised models")
        ##  one whole dataframe concatnated in a dict
        ##  Standardization is done so data can actually be appended
        self.raw_data = bars.get_data()  ## a dict(pd.DataFrame)
        if shift > 0:
            self.shift = -shift
        else:
            self.shift = shift

    def get_shift(self):
        return abs(self.shift)

    # returns dict(pd.DataFrame)
    def preprocess_X(self, bars):
        print("Preprocessing X: StandardScalar")
        X = {}
        scaler = StandardScaler()
        for k,v in bars.items():  # k is ticker name, v is df
            temp_data = []
            for t in range(1,v.shape[0]):                
                start = t-abs(self.shift*2) if t-abs(self.shift*2) > 0 else 0 
                sliced = v.iloc[start:t,:] 
                temp_data.append(scaler.fit_transform(sliced)[-1,:])
            temp_df = pd.DataFrame(temp_data, columns=v.columns)
            temp_df = self.transform_X(temp_df)
            X[k] = temp_df
        return X
    
    def transform_X(self, df):
        ## no feature engineering done
        return df
    
    def preprocess_Y(self, X):
        ## derive Y from transformed X
        ## In this basic example, our reference is the price self.shift days from now.
        Y = {}
        for k,v in X.items():
            temp_ser = v["Close"].shift(self.shift)
            Y[k] = temp_ser
        return Y
    
    # must be implemented
    ## appends all data into 1 large dataframe with extra col - ticker
    ## returns (pd.DataFrame, pd.Series)
    def _process_data(self):
        X = self.transform_X(self.preprocess_X(self.raw_data))
        Y = self.preprocess_Y(X)
        dfs = []
        for symbol in X.keys():    
            assert X[symbol].shape[0] == Y[symbol].shape[0]
            temp_df = X[symbol].copy()
            temp_df["target"] = Y[symbol]
            dfs.append(temp_df)
        all_df = pd.concat(dfs, axis=0, ignore_index=True).dropna()
        return (all_df.drop('target',axis=1), all_df['target']) 

class DataWithLag(BaseSkData):
    def __init__(self, bars, shift, lag:int):
        super().__init__(bars, shift)
        self.lag = lag + 1
    
    def transform_X(self, df):
        ## obtains lagged data for lag days
        X = {}
        for k,v in self.raw_data.items():
            scaler = StandardScaler()
            temp_df = scaler.fit_transform(v)
            temp_df = pd.DataFrame(temp_df, columns=v.columns)
            for i in range(1,self.lag):
                temp_df["lag_"+str(i)] = temp_df["Close"].shift(-i)
            X[k] = temp_df
        return X

class ClassificationData(SKData):
    pass