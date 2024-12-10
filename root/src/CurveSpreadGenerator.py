# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 11:18:28 2024

@author: Diego
"""

import os
import pandas as pd

from DataCollect import VIXCurveDataCollector

class VIXCurveSpread(VIXCurveDataCollector):
    
    def __init__(self) -> None:
        
        super().__init__()
        self.spread_path = os.path.join(self.data_path, "CurveSpread")
        self.short_legs  = ["UX1", "UX2"]
        
        if os.path.exists(self.spread_path) == False: os.makedirs(self.spread_path)
        
    def _get_spread(self, df: pd.DataFrame, short_leg: str) -> pd.DataFrame:
        
        df_out = (df.drop(
            columns = ["PX_LAST"]).
            pivot(index = "date", columns = "security", values = "px_diff").
            reset_index().
            melt(id_vars = ["date", short_leg]).
            dropna().
            rename(columns = {
                short_leg : "short_rtn",
                "value"   : "long_rtn"}).
            assign(
                short_leg = short_leg,
                strat     = lambda x: x.security + "-" + short_leg))
        
        return df_out
            
    def get_all_spreads(self) -> pd.DataFrame: 
        
        df_curve = self.get_vix_curve()
        
        df_out = (pd.concat([
            self._get_spread(df_curve, short_leg)
            for short_leg in self.short_legs]).
            rename(columns = {"security": "long_leg"}).
            query("strat != 'UX1-UX2'"))
        
        return df_out
    
    def generate_equal_spread(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.spread_path, "EqualSpread.parquet")
        try:
            
            if verbose == True: print("Trying to find equal spread data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find data, generating equal weight spread")
            df_out = (self.get_all_spreads().assign(
                spread = lambda x: 0.5 * (x.long_rtn - x.short_rtn))
                [["date", "strat", "short_leg", "spread"]])
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        
        return df_out

def main() -> None:
        
    VIXCurveSpread().generate_equal_spread()
    
if __name__ == "__main__": main()