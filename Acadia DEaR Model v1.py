# -*- coding: utf-8 -*-
"""
Created on Mon Jul 17 09:12:24 2017

@author: KevinMorgan
"""

#%% imports
from __future__ import print_function
from datetime import date, timedelta
import pandas.io.sql as psql
from sqlalchemy import create_engine
import pandas as pd
from dateutil import relativedelta
import numpy as np
import QuantLib as ql
import AcadiaHelperFunctions as ahf
import timeit

#%% initialize

# start timer
start = timeit.timeit()

# today
dt = date.today()
# dt = date(2017, 7, 13)

# number of simulations to run
num_sims = 500

#%% +++++++++++++++++++++++++++
# set up database connection with SQLAlchemy
# +++++++++++++++++++++++++++

# setup sqlalchemy's connection to database
string_driver = 'mysql+mysqlconnector://root:Acadia@localhost/AcadiaRisk'
engine = create_engine(string_driver)
cnx = engine.connect()

# get holidays list of list and flatten with the "sum" trick
# (note: not efficient for very large datasets)
string_query = """Select HolidayDate from holiday where Exchange = 3"""
holidays = sum(psql.read_sql_query(string_query, cnx).values.tolist(), [])

# previous business day
dt_1 = ahf.date_by_subtracting_business_days(dt, 1, holidays)


#%% ++++++++++++ Data Set Construction +++++++++++++++++
# convert forward dates to promptness
# ++++++++++++++++++++++++++++++++++++++++++++++++++++

# read current portfolio for 'live' tickers only
query = """select
HX.TradeDate,
concat(HX.Ticker, "-", HX.ForwardDate) Datum,
HX.Price
from
(select
PC.Ticker,
PC.TradeDate,
PC.ForwardDate,
PC.Price
from
(SELECT
TB.Ticker,
TB.ContractStart,
Sum(TB.Quantity)
FROM
trade_blotter TB
where TB.ContractStart > curdate()
GROUP BY TB.Ticker, TB.ContractStart
HAVING (Sum(TB.Quantity) != 0)
) TBA
inner join price_curves PC on (PC.Ticker = TBA.Ticker and
PC.ForwardDate = TBA.ContractStart)
UNION ALL
select
PC.Ticker,
PC.TradeDate,
PC.ForwardDate,
PC.Price
from
(Select
OU.Underlying_TickerId,
TB.ContractStart,
TB.CP,
TB.Strike,
sum(TB.Quantity)
from
trade_blotter TB
inner join option_to_underlying OU on (OU.Option_TickerId = TB.Ticker)
where TB.ContractStart > curdate()
group by OU.Underlying_TickerId, TB.ContractStart, TB.CP, TB.Strike
Having sum(TB.Quantity) != 0 ) TBB
inner join price_curves PC on (PC.Ticker = TBB.Underlying_TickerId and
PC.ForwardDate = TBB.ContractStart)) HX
group by HX.Ticker, HX.TradeDate, HX.ForwardDate, HX.Price"""

# get historical prices
df_historical_data = psql.read_sql_query(query, cnx)

# create pivoted data set
df_historical_data_pivot = df_historical_data.pivot(index='TradeDate',
                                                    columns='Datum',
                                                    values='Price')

# take daily diff
df_diffs = df_historical_data_pivot.diff(periods=1, axis=0)

# dataframe set to zero mean
df_diffs = df_diffs - df_diffs.mean(axis=0, skipna=True)

# create a correlation dataframe
df_diffs_correlation = df_diffs.corr()
df_diffs_covariance = df_diffs.cov()


#%% ++++++ PCA Simulations  +++++++++

# calculate eigenvalues and eigenvectors
evals, evecs = np.linalg.eigh(df_diffs_covariance.as_matrix())

# get standard deviaion for each time series
sigma = np.sqrt(np.diag(df_diffs_covariance.as_matrix()))

# trim out negative evals
evals[evals < 0] = 0.0001

# draw random numbers
innovations = []
for i in range(num_sims):
    # randomly draw normal variants
    ran_temp = np.random.normal(0, 1, len(sigma)) * np.array(np.sqrt(evals))
    innovations.append(ran_temp)
    innovations.append(-1 * ran_temp)

# convert innovations back to original timeseries space
innovations_x = np.linalg.multi_dot([np.array(innovations),
                                     np.linalg.inv(evecs)])

# six sigma stress scenario
# stress = np.ones(len(sigma)) * 6 * np.array(np.sqrt(evals))
# innovation_stress = np.linalg.multi_dot([stress, np.linalg.inv(evecs)])

innovations_out = (np.asarray(innovations_x) * sigma)

# create scenario dataframe, appending antithetic values
df_scenarios = pd.DataFrame(np.asarray(innovations_out.T),
                            index=df_diffs_correlation.index)

#%%  +++++++++ Calculate Linear PnL ++++++++++++++++++++
                                       
# Linear portfolio query
query = """select
TBB.TradeId,
TBB.LegId,
TBB.Ticker,
TBB.ContractStart,
TBb.FCMAccount,
concat(TBB.Ticker, "-", TBB.ContractStart) Datum,
TBB.Quantity
from
(SELECT
TB.Ticker,
TB.ContractStart,
Sum(TB.Quantity)
FROM
trade_blotter TB
GROUP BY TB.Ticker, TB.ContractStart
HAVING (Sum(TB.Quantity) != 0)
) TBA
inner join trade_blotter TBB on (TBB.Ticker = TBA.Ticker and
TBB.ContractStart = TBA.ContractStart)
where TBB.CP is null and TBB.ContractStart > curdate();"""

df_linear = psql.read_sql_query(query, cnx)
df_linear['PnL'] = pd.Series(np.zeros(len(df_linear)),
                             index=df_linear.index)
df_linear['Scenario'] = pd.Series(np.zeros(len(df_linear)),
                                  index=df_linear.index)
df_linear.to_sql("var", cnx, if_exists='replace')

for i in range(df_scenarios.shape[1]):
    df_linear['PnL'] = pd.Series(np.zeros(len(df_linear)),
                                 index=df_linear.index)
    for j in range(len(df_linear)):
        df_linear.loc[j, 'PnL'] = (df_linear['Quantity'][j] *
                                   df_scenarios[i][df_linear['Datum'][j]])
        df_linear.loc[j, 'Scenario'] = i
    df_linear.to_sql("var", cnx, if_exists='append')


#%% +++++++ Calculate Nonlinear PnL ++++++++++++++

# NonLinear portfolio query
query = """select
TBB.TradeDate,
TBB.TradeId,
TBB.LegId,
TBB.Ticker,
TBB.ContractStart,
TBB.FCMAccount,
concat(OU.Underlying_TickerId, "-", TBB.ContractStart) Datum,
TBB.Quantity,
TBB.CP,
TBB.Strike,
TBB.TradePrice,
OC.ImpVol,
OC.Premium,
OC.Expiry,
PC.Ticker,
PC.Price
from
(SELECT
TB.Ticker,
TB.ContractStart,
Sum(TB.Quantity)
FROM
trade_blotter TB
GROUP BY TB.Ticker, TB.ContractStart
HAVING (Sum(TB.Quantity) != 0)
) TBA
inner join trade_blotter TBB on (TBB.Ticker = TBA.Ticker and
TBB.ContractStart = TBA.ContractStart)
inner join option_to_underlying OU on (OU.Option_TickerId = TBB.Ticker)
inner join options_curves OC on (OC.Ticker = OU.Option_TickerId and
OC.ForwardDate = TBB.ContractStart and OC.CP = TBB.CP and
OC.Strike = TBB.Strike)
inner join price_curves PC on (PC.Ticker = OU.Underlying_TickerId and
PC.ForwardDate = TBB.ContractStart and PC.TradeDate = OC.TradeDate)
where TBB.CP is not null and
OC.TradeDate = '""" + dt_1.strftime("%Y-%m-%d") + """'"""

df_nonlinear = psql.read_sql_query(query, cnx)

df_nonlinear['PnL'] = pd.Series(np.zeros(len(df_linear)),
                                index=df_linear.index)
df_nonlinear['Scenario'] = pd.Series(np.zeros(len(df_linear)),
                                     index=df_linear.index)

# Calculate Delta using QuantLib helper function 'Black76'
for i in range(df_scenarios.shape[1]):
    df_nonlinear['PnL'] = pd.Series(np.zeros(len(df_linear)),
                                    index=df_linear.index)
    for j in range(len(df_nonlinear)):
        change_in_price = df_scenarios[i][df_nonlinear['Datum'][j]]
        optprem = ahf.Black76(df_nonlinear['CP'][j],
                              df_nonlinear['Price'][j] + change_in_price,
                              df_nonlinear['Strike'][j],
                              df_nonlinear['ImpVol'][j] / 100,
                              0.0237,
                              ql.DateParser.parseFormatted(
                                    dt_1.strftime('%Y-%m-%d'), '%Y-%m-%d'),
                              ql.DateParser.parseFormatted(
                                    df_nonlinear['Expiry'][j].
                                    strftime('%Y-%m-%d'), '%Y-%m-%d'),
                              'premium')
#        print('Calculated Premium: ', optprem,
#              '  Setteled  :', df_nonlinear['Premium'][j], 
#              ' Price Change: ', change_in_price)
        
        pnl = (df_nonlinear['Premium'][j] -
               optprem) * df_nonlinear['Quantity'][j]
        df_nonlinear.loc[j, 'PnL'] = pnl
        df_nonlinear.loc[j, 'Scenario'] = i
    (df_nonlinear.loc[:, ['TradeId', 'Legid', 'Ticker', 'ContractStart',
                          'FCMAccount', 'Datum', 'Quantity', 'PnL',
                          'Scenario']]).to_sql("var", cnx, if_exists='append')




#%% +++++++ Close database connection
    
print("Took:  ", timeit.timeit() - start,
      " to complete for number of sims ", num_sims)

cnx.close()





















