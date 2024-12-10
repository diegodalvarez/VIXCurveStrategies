#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 16:19:53 2024

@author: diegoalvarez
"""
import os
import pandas as pd

class VIXCurveDataCollector:
    
    def __init__(self) -> None:
        
        self.root_path = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
        self.repo_path = os.path.abspath(os.path.join(self.root_path, os.pardir))
        self.data_path = os.path.join(self.repo_path, "data")
        self.raw_path  = os.path.join(self.data_path, "RawData")
        
        if os.path.exists(self.data_path) == False: os.makedirs(self.data_path)
        if os.path.exists(self.raw_path)  == False: os.makedirs(self.raw_path)
        
        self.bbg_fut_path = r"/Users/diegoalvarez/Desktop/BBGFuturesManager/data"
        if os.path.exists(self.bbg_fut_path) == False: 
            self.bbg_fut_path = r"C:\Users\Diego\Desktop\app_prod\BBGFuturesManager\data"
            
        self.bbg_path = r"C:\Users\Diego\Desktop\app_prod\BBGData\data"
        
        self.max_contract = 5
        self.misc_tickers = ["VIX", "VVIX"]
        
    def _get_diff(self, df: pd.DataFrame) -> pd.DataFrame: 
        
        df_out = (df.sort_values(
            "date").
            assign(px_diff = lambda x: x.PX_LAST.diff()).
            dropna())
        
        return df_out
        
    def get_vix_curve(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.raw_path, "UXCurve.parquet")
        try:
            
            if verbose == True: print("Trying to find VIX Curve Data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find data, collecting it")
            front_path = os.path.join(self.bbg_fut_path, "PXFront", "UX.parquet")
            back_paths = [
                os.path.join(self.bbg_fut_path, "PXBack", str(contract), "UX.parquet")
                for contract in range(2, self.max_contract + 1)]
            
            paths  = [front_path] + back_paths
            df_out = (pd.read_parquet(
                path = paths, engine = "pyarrow").
                assign(
                    security = lambda x: x.security.str.split(" ").str[0],
                    date     = lambda x: pd.to_datetime(x.date).dt.date).
                groupby("security").
                apply(self._get_diff).
                reset_index(drop = True))
    
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_out
    
    def get_misc_data(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.raw_path, "MiscData.parquet")
        try:
            
            if verbose == True: print("Trying to find Misc Data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except: 
            
            if verbose == True: print("Couldn't find MISC data, collecting it now")
            paths = [
                os.path.join(self.bbg_path, ticker + ".parquet")
                for ticker in self.misc_tickers]
            
            df_out = (pd.read_parquet(
                path = paths, engine = "pyarrow").
                assign(
                    date     = lambda x: pd.to_datetime(x.date).dt.date,
                    security = lambda x: x.security.str.split(" ").str[0]).
                drop(columns = ["variable"]).
                rename(columns = {"value": "PX_LAST"}).
                groupby("security").
                apply(self._get_diff).
                reset_index(drop = True))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
def main() -> None:
    
     VIXCurveDataCollector().get_vix_curve(verbose = True)
     VIXCurveDataCollector().get_misc_data(verbose = True)
     
if __name__ == "__main__": main()