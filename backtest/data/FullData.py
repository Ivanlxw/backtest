import os
import pandas as pd

from backtest.data.dataHandler import HistoricCSVDataHandler

# class CSVDataCreater:
#     def __init__(self, csv_dir, symbol_list, start_date, end_date=None, *args):
#         """
#         Get the full dataset in pd.Dataframe from CSV files
#         Args:
#         - relative path of the CSV files 
#         - a list of symbols determining universal stocks

#         Returns: dict(pd.DataFrame)

#         """
#         self.csv_dir = csv_dir
#         self.symbol_list = symbol_list
#         self.start_date = start_date
#         self.end_date = end_date
#         self.symbol_data = {}

#         self._open_convert_csv_files()

#     def _open_convert_csv_files(self):
#         comb_index = None
#         for s in self.symbol_list:
#             temp = pd.read_csv(
#                 os.path.join(self.csv_dir, f"{s}.csv"),
#                 header = 0, index_col= 0,
#             ).drop_duplicates()
#             temp.columns = ["Open", "High", "Low", "Close", "Volume"]
#             if self.end_date == None:
#                 filtered = temp.iloc[temp.index.get_loc(self.start_date):,]
#             else:
#                 filtered = temp.iloc[temp.index.get_loc(self.start_date):temp.index.get_loc(self.end_date),]
#             self.symbol_data[s] = filtered

#             ## combine index to pad forward values
#             if comb_index is None:
#                 comb_index = self.symbol_data[s].index
#             else: 
#                 comb_index.union(self.symbol_data[s].index.drop_duplicates())
                    
#         ## reindex
#         for s in self.symbol_list:
#             self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index, method='pad')

#     def get_data(self,):
#         return self.symbol_data

class CSVDataCreater(HistoricCSVDataHandler):
    def __init__(self, csv_dir, symbol_list, start_date, end_date=None, *args):
        super().__init__(None, csv_dir, symbol_list, start_date, end_date)
    
    def get_data(self):
        return self.symbol_data
