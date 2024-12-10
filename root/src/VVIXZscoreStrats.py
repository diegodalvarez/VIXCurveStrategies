# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 11:16:57 2024

@author: Diego
"""

import os
import numpy as np
import pandas as pd

from CurveSpreadGenerator import VIXCurveSpread

class VVIXZscoreStrats(VIXCurveSpread):
    
    def __init__(self) -> None:
        
        super().__init__()
        
        self.strat_path = os.path.join(self.data_path, "VVIXZscoreStrats")
        if os.path.exists(self.strat_path) == False: os.makedirs(self.strat_path)
        
        self.z_score_windows = [5, 10, 20]
        
    def _get_zscore(self, df: pd.DataFrame, window: int) -> pd.DataFrame: 
        
        df_out = (df.sort_values(
            "date").
            assign(
                roll_mean  = lambda x: x.PX_LAST.ewm(span = window, adjust = False).mean(),
                roll_std   = lambda x: x.px_diff.ewm(span = window, adjust = False).std(),
                z_score    = lambda x: (x.PX_LAST - x.roll_mean) / x.roll_std,
                lag_zscore = lambda x: x.z_score.shift(),
                window     = window))
        
        return df_out
        
    def get_zscore(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.strat_path, "VVIXEqualSpread.parquet")
        try:
            
            if verbose == True: print("Searching for VVIX Z-Score Spread Data")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found Data\n")
            
        except: 
            
            if verbose == True: print("Couldn't find data, generating VVIX Z-Score")
        
            df_tmp = (self.get_misc_data().query(
                "security == 'VVIX'").
                drop(columns = ["security"]))
            
            df_signal = (pd.concat([
                self._get_zscore(df_tmp, window)
                for window in self.z_score_windows]).
                dropna()
                [["date", "window", "lag_zscore"]])
            
            df_out = (df_signal.merge(
                right = self.generate_equal_spread(), how = "inner", on = ["date"]).
                assign(signal_spread = lambda x: np.sign(x.lag_zscore) * x.spread))
            
            if verbose == True: print("Saving Data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_out
    
def main() -> None:
    
    VVIXZscoreStrats().get_zscore(verbose = True)
    
if __name__ == "__main__": main()