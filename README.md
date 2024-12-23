# VIXCurveStrategies
VIX Futures Curve Strategy. The overall approach of this model is to try to dynamically position VIX carry positions. While the VIX futures carry isn't a new idea and the construction of such trades are quite straight forward this repo will focus on optimizing the returns of the VIX futures. In this case the repo will rely on markov regime models based on positioning. All the returns are calculated in PnL rather than percent return due to the nature of VIX roll adjusted futures and changes. 

First begin with the generic positioning PnL of the data
<img width="1427" alt="image" src="https://github.com/user-attachments/assets/d8f3aaff-65d2-419b-8656-408a5f98e2db" />

Then from there construct equal risk contribution portfolio by using inverse volatilty weighting
<img width="993" alt="image" src="https://github.com/user-attachments/assets/c3996e76-5eb7-4107-add7-6f147e6a301d" />

Volatility-targeting doesn't generate significant outperformance. Which prompts the use of some regime based method.
<img width="1183" alt="image" src="https://github.com/user-attachments/assets/18f32f65-bfab-4486-8159-611255a5064d" />


## Repo Setup
1. CurveGenerator -> Code to generate spread between strategies
2. Strats -> VVIX Zscore (trading zscore of VVIX), markov strategies (using markov regime setup)

## Todo
1. VVIX Markov Regression notebook
2. In-Sample Out-of-Sample Regression
3. Portfolio Optimization of VVIX and Markovian Strategies (ERC, rolling sharpe max)
4. Make Master Strat
5. Need to make switching variance of VIX diff markov process
6. Need to make markov process with external endogenous factor of VVIX
7. Probability Smoothing for transaction turnover
