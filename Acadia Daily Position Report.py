# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 12:53:41 2017

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
dt_2 = ahf.date_by_subtracting_business_days(dt, 2, holidays)

#%% ++++++ Linear Positions +++++++++++++++++++++

# query statement
query = """select
TB.TradeId,
TB.TradeDate,
P.Last,
TM.Ticker,
TM.AcadiaName,
TB.ContractStart,
TM.frequency,
TB.Quantity,
CT.CommodType,
TB.CP,
TB.Strike,
TM.dealtype,
TB.Quantity as Delta,
0 as Vega,
0 as Gamma,
0 as Theta
from trade_blotter TB
inner join tickermappings TM on (TM.tickerId = TB.Ticker)
inner join personnel P on (P.FCMAccount = TB.FCMAccount)
inner join dim_commodtype CT on (CT.commodtype_id = TM.commodtype)
where TM.dealtype not in (7)"""

# create dataframe from sql
df_position = psql.read_sql_query(query, cnx)

# Add Linear Non Linear tagging
df_position['NonLinear'] = pd.Series(np.zeros(len(df_position)),
                           index=df_position.index)

# clean/create database table, then add data
df_position.to_sql("position", cnx, if_exists='replace')


#%% +++++++ Plain Vanilla Options Positions  ++++++++

# Plain Vanilla Options
query = """select
TB.TradeId,
TB.TradeDate,
P.Last,
TM.Ticker,
TM.AcadiaName,
TB.ContractStart,
TM.frequency,
TB.Quantity,
CT.CommodType,
TM.dealtype,
TB.CP,
TB.Strike,
OC.Expiry,
OC.ImpVol,
PC.Price as Underlying
from trade_blotter TB
inner join tickermappings TM on (TM.tickerId = TB.Ticker)
inner join options_curves OC on (OC.Ticker = TB.Ticker and
OC.Strike = TB.Strike and
OC.CP = TB.CP and OC.ForwardDate = TB.ContractStart)
inner join price_curves PC on (PC.Ticker = OC.UnderlyingTickerId and
PC.TradeDate = OC.TradeDate and PC.ForwardDate = OC.ForwardDate)
inner join personnel P on (P.FCMAccount = TB.FCMAccount)
inner join dim_commodtype CT on (CT.commodtype_id = TM.commodtype)
where TM.dealtype in (7) and
PC.TradeDate = '""" + dt_1.strftime("%Y-%m-%d") + """' and
OC.TradeDate = '""" + dt_1.strftime("%Y-%m-%d") + """' and
OC.Expiry > '""" + dt_1.strftime("%Y-%m-%d") + """'"""

df_options = psql.read_sql_query(query, cnx)

# Calculate Delta from QuantLib helper function 'Black76'
temp = []
for i in range(len(df_options)):
    temp.append(ahf.Black76(df_options['CP'][i],
                            df_options['Underlying'][i],
                            df_options['Strike'][i],
                            df_options['ImpVol'][i] / 100,
                            0.0237,
                            ql.DateParser.parseFormatted(
                                    dt_1.strftime('%Y-%m-%d'), '%Y-%m-%d'),
                            ql.DateParser.parseFormatted(
                                    df_options['Expiry'][i].
                                    strftime('%Y-%m-%d'), '%Y-%m-%d'),
                            'delta') * df_options['Quantity'][i])

# append delta
df_options['Delta'] = pd.Series(temp, index=df_options.index)

# Calculate vegas from QuantLib helper function 'Black76'
temp = []
for i in range(len(df_options)):
    temp.append(ahf.Black76(df_options['CP'][i],
                            df_options['Underlying'][i],
                            df_options['Strike'][i],
                            df_options['ImpVol'][i] / 100,
                            0.0237,
                            ql.DateParser.parseFormatted(
                                    dt_1.strftime('%Y-%m-%d'), '%Y-%m-%d'),
                            ql.DateParser.parseFormatted(
                                    df_options['Expiry'][i].
                                    strftime('%Y-%m-%d'), '%Y-%m-%d'),
                            'vega') * df_options['Quantity'][i] / 100)

# append vega
df_options['Vega'] = pd.Series(temp, index=df_options.index)

# Calculate gamma from QuantLib helper function 'Black76'
temp = []
for i in range(len(df_options)):
    temp.append(ahf.Black76(df_options['CP'][i],
                            df_options['Underlying'][i],
                            df_options['Strike'][i],
                            df_options['ImpVol'][i] / 100,
                            0.0237,
                            ql.DateParser.parseFormatted(
                                    dt_1.strftime('%Y-%m-%d'), '%Y-%m-%d'),
                            ql.DateParser.parseFormatted(
                                    df_options['Expiry'][i].
                                    strftime('%Y-%m-%d'), '%Y-%m-%d'),
                            'gamma') * df_options['Quantity'][i])

# append gamma
df_options['Gamma'] = pd.Series(temp, index=df_options.index)

# Calculate theta from QuantLib helper function 'Black76'
temp = []
for i in range(len(df_options)):
    temp.append(ahf.Black76(df_options['CP'][i],
                            df_options['Underlying'][i],
                            df_options['Strike'][i],
                            df_options['ImpVol'][i] / 100,
                            0.0237,
                            ql.DateParser.parseFormatted(
                                    dt_1.strftime('%Y-%m-%d'), '%Y-%m-%d'),
                            ql.DateParser.parseFormatted(
                                    df_options['Expiry'][i].
                                    strftime('%Y-%m-%d'), '%Y-%m-%d'),
                            'theta') * df_options['Quantity'][i] / 365)

# append theta
df_options['Theta'] = pd.Series(temp, index=df_options.index)


# Add Linear, Non Linear tagging
df_options['NonLinear'] = pd.Series(np.ones(len(df_options)),
                                    index=df_options.index)

# set dataframe to proper subset
df_options = df_options.loc[:, lambda df: ['TradeId',
                                           'TradeDate',
                                           'Last',
                                           'Ticker',
                                           'AcadiaName',
                                           'ContractStart',
                                           'frequency',
                                           'Quantity',
                                           'CommodType',
                                           'dealtype',
                                           'CP',
                                           'Strike',
                                           'Delta',
                                           'Vega',
                                           'Gamma',
                                           'Theta',
                                           'NonLinear']]

# append to position table
df_options.to_sql("position", cnx, if_exists='append')





#%% ++++ Close database connection ++++
cnx.close()
