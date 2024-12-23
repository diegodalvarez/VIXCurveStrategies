# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 11:59:43 2024

@author: Diego
"""

import os
import numpy as np
import pandas as pd

from CurveSpreadGenerator import VIXCurveSpread
from statsmodels.tsa.regime_switching.markov_regression import MarkovRegression

class MarkovianStrats(VIXCurveSpread):
    
    def __init__(self) -> None:
        
        super().__init__()
        
        self.strat_path = os.path.join(self.data_path, "MarkovianStrats")
        if os.path.exists(self.strat_path) == False: os.makedirs(self.strat_path)
        
    def _get_vix(self) -> pd.DataFrame: 
     
        df_out = (self.get_misc_data().query(
            "security == 'VIX'").
            drop(columns = ["security", "px_diff"]))
        
        return df_out
        
    def generate_generic_markovian_prob(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.strat_path, "GenericMarkovProbability.parquet")
        try:
            
            if verbose == True: print("Trying to find Generic Markov Regression")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
        
            if verbose == True: print("Couldn't find any info, generating generic markov data")    
        
            df_out = (MarkovRegression(
                endog     = self._get_vix().set_index("date").PX_LAST, 
                k_regimes = 2).
                fit().
                smoothed_marginal_probabilities.
                shift().
                reset_index().
                melt(id_vars = "date").
                assign(regime = lambda x: "regime" + (x.variable + 1).astype(str)).
                drop(columns = ["variable"]).
                dropna().
                rename(columns = {"value": "prob"}))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
    def generate_lagged_markovian_prob(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.strat_path, "LaggedMarkovProbability.parquet")
        try:
            
            if verbose == True: print("Trying to find Lagged Markov Regression")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except:
            
            vix = self._get_vix().set_index("date").PX_LAST
            if verbose == True: print("Couldn't find any info, generating lagged markov data")
            
            df_out = (MarkovRegression(
                endog     = vix[1:],
                k_regimes = 2,
                exog      = vix.iloc[:-1]).
                fit().
                smoothed_marginal_probabilities.
                shift().
                reset_index().
                melt(id_vars = "date").
                assign(regime = lambda x: "regime" + (x.variable + 1).astype(str)).
                drop(columns = ["variable"]).
                dropna().
                rename(columns = {"value": "prob"}))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out
    
    def generate_lagged_vvix_markovian_prob(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.strat_path, "LaggedVVIXMarkovProbability.parquet")
        try:
            
            if verbose == True: print("Trying to find Lagged VVIX Markov Regression")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except:
            
            if verbose == True: print("Couldn't find any info, generating lagged markov data")
            df = (self.get_misc_data().drop(
                columns = ["px_diff"]).
                pivot(index = "date", columns = "security", values = "PX_LAST").
                dropna())
            
            df_out = (MarkovRegression(
                endog     = df.VIX[1:],
                k_regimes = 2,
                exog      = df[:-1]).
                fit().
                smoothed_marginal_probabilities.
                shift().
                reset_index().
                melt(id_vars = "date").
                assign(regime = lambda x: "regime" + (x.variable + 1).astype(str)).
                drop(columns = ["variable"]).
                dropna().
                rename(columns = {"value": "prob"}))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
        
        return df_out
    
    def generate_signal_rtn(self, verbose: bool = False) -> pd.DataFrame: 
        
        file_path = os.path.join(self.strat_path, "MarkovReturns.parquet")
        try:
            
            if verbose == True: print("Trying to find Markov Returns")
            df_out = pd.read_parquet(path = file_path, engine = "pyarrow")
            if verbose == True: print("Found data\n")
            
        except: 
            
            if verbose == True: print("Couldn't find Markov Returns data")
            
            df_generic = (self.generate_generic_markovian_prob().query(
                "regime == regime.min()").
                drop(columns = ["regime"]).
                assign(markov = "generic").
                assign(position = lambda x: np.where(x.prob > 0.5, 1, -1)))
            
            df_lagged = (self.generate_lagged_markovian_prob().query(
                "regime == regime.min()").
                drop(columns = ["regime"]).
                assign(markov = "lagged").
                assign(position = lambda x: np.where(x.prob < 0.5, 1, -1)))
            
            df_signals = pd.concat([df_generic, df_lagged])
            df_out = (self.generate_equal_spread().merge(
                right = df_signals, how = "inner", on = ["date"]).
                assign(signal_spread = lambda x: x.position * x.spread))
            
            if verbose == True: print("Saving data\n")
            df_out.to_parquet(path = file_path, engine = "pyarrow")
            
        return df_out

def main() -> None:
    
    df = MarkovianStrats().generate_generic_markovian_prob(verbose = True)
    df = MarkovianStrats().generate_lagged_markovian_prob(verbose = True)
    df = MarkovianStrats().generate_signal_rtn(verbose = True)
    df = MarkovianStrats().generate_lagged_vvix_markovian_prob(verbose = True)
    
if __name__ == "__main__": main()