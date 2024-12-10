# -*- coding: utf-8 -*-
"""
Created on Tue Dec 10 12:36:47 2024

@author: Diego
"""

import os
import pandas as pd
import matplotlib.pyplot as plt

class BackgroundFunctions:
    
    def plot_backtest_rtn(self, df: pd.DataFrame) -> plt.Figure: 
        
        ncols      = len(df.short_leg.drop_duplicates().to_list())
        nrows      = len(df.param.drop_duplicates().to_list())
        group_vars = df.group_var.drop_duplicates().sort_values().to_list()
        
        fig, axes = plt.subplots(ncols = ncols, nrows = nrows, figsize = (20, nrows * 4))
        for group_var, ax in zip(group_vars, axes.flatten()): 
            
            (df.query(
                "group_var == @group_var")
                [["date", "strat", "signal_spread"]].
                rename(columns = {"strat": ""}).
                pivot(index = "date", columns = "", values = "signal_spread").
                cumsum().
                plot(
                    ax = ax,
                    title = group_var,
                    ylabel = "Cumulative PnL"))
            
        fig.suptitle("Strategy Cumulative PnL from {} to {}".format(
            df.date.min(),
            df.date.max()))
        
        plt.tight_layout()