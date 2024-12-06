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
        
        self.max_contract = 5
        
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
                    date     = lambda x: pd.to_datetime(x.date).dt.date))
            
            if verbose == True: print("Saving data\n")
        
        return df_out
    
def main() -> None:
    
     VIXCurveDataCollector().get_vix_curve(verbose = True)
     
if __name__ == "__main__": main()