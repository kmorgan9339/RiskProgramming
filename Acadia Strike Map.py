# -*- coding: utf-8 -*-
"""
Created on Wed Aug  2 12:24:38 2017

@author: KevinMorgan
"""

#%% ++++++++ Imports
from __future__ import print_function
from datetime import date
import pandas.io.sql as psql
from sqlalchemy import create_engine
import pandas as pd
import AcadiaHelperFunctions as ahf
import numpy as np
import QuantLib as ql

#%% ++++++ Initialize ++++++++++

# today
dt = date.today()
# dt = date(2017, 7, 31)

#%% ++++++++ Initialize +++++++++
""" """
# +++++++++++++++++++++++++++++++

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


#%% +++++++ Power PJM Monthy/Annual ++++++++

# Make Y headers
datelist = pd.date_range(date(dt.year, dt.month + 1, 1),
                         date(dt.year + 3, dt.month + 1, 1), freq='MS').date

# Make X headers
strikelist = np.arange(20, 100, 0.50)


# Yesterday
query = """select
TB.ContractStart,
TB.Strike,
TB.Quantity
from trade_blotter TB
where
TB.ContractStart > '""" + dt_1.strftime("%Y-%m-%d") + """' and
TB.Ticker in (113, 128)
order by TB.ContractStart, TB.Quantity"""

df_options = psql.read_sql_query(query, cnx)

q = np.zeros((len(strikelist), len(datelist)))

for i in range(len(datelist) - 1):
    for j in range(len(strikelist) - 1):
        for k in range(len(df_options)):
            if (df_options['ContractStart'][k] >= datelist[i] and
                    df_options['ContractStart'][k] < datelist[i + 1] and
                    df_options['Strike'][k] >= strikelist[j] and
                    df_options['Strike'][k] < strikelist[j + 1]):
                q[j, i] = q[j, i] + df_options['Quantity'][k]


df_power_strike_map = pd.DataFrame(q, index=strikelist,
                                   columns=datelist).sort_index(
                                           ascending=False)

#%% +++++++ Natural Gas Monthy ++++++++

# Make Y headers
datelist = pd.date_range(date(dt.year, dt.month + 1, 1),
                         date(dt.year + 3, dt.month + 1, 1), freq='MS').date

# Make X headers
strikelist = np.arange(2, 4, 0.01)


# Yesterday
query = """select
TB.ContractStart,
TB.Strike,
TB.Quantity
from trade_blotter TB
where 
TB.ContractStart > '""" + dt_1.strftime("%Y-%m-%d") + """' and
TB.Ticker = 98
order by TB.ContractStart, TB.Quantity"""

df_options = psql.read_sql_query(query, cnx)

# create strike map
q = np.zeros((len(strikelist),len(datelist)))
for i in range(len(datelist) - 1):
    for j in range(len(strikelist) - 1):
        for k in range(len(df_options)):
            if (df_options['ContractStart'][k] >= datelist[i] and
                    df_options['ContractStart'][k] < datelist[i + 1] and
                    df_options['Strike'][k] >= strikelist[j] and
                    df_options['Strike'][k] < strikelist[j + 1]):
                q[j, i] = q[j, i] + df_options['Quantity'][k]


# put matrix into dataframe
df_natgas_strike_map = pd.DataFrame(q, index=strikelist,
                                   columns=datelist).sort_index(
                                           ascending=False)



#%% +++++++  Close Database Connection
cnx.close()


















