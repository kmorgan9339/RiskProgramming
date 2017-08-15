# -*- coding: utf-8 -*-
"""
Created on Thu Jul 13 13:48:53 2017

@author: KevinMorgan
"""

from __future__ import print_function
from datetime import date
import pandas.io.sql as psql
from sqlalchemy import create_engine
import pandas as pd
import AcadiaHelperFunctions as ahf


# today
dt = date.today()
# dt = date(2017, 7, 28)

# setup sqlalchemy's connection to database
string_driver = 'mysql+mysqlconnector://root:Acadia@localhost/AcadiaRisk'
engine = create_engine(string_driver)
cnx = engine.connect()

# get holidays list of list and flatten with the "sum" trick
# (note: not efficient for very large datasets)
string_query = """Select HolidayDate from holiday where Exchange = 3"""
holidays = sum(psql.read_sql_query(string_query, cnx).values.tolist(), [])

# get tickermappings into dictionary from dataframe
string_query = """select Ticker, tickerId
from tickermappings
order by Ticker;"""
df_tickermappings = psql.read_sql_query(string_query, cnx)
dict_tickermappings = df_tickermappings.set_index('Ticker').to_dict()

# create simple CP dictionary
dict_cp = {'call': 'C', 'put': 'P'}

# previous business day
dt_1 = ahf.date_by_subtracting_business_days(dt, 1, holidays)

# path & filename of molecule pnl file
path = "C:/Users/KevinMorgan/Downloads/"
filename = "pnl-" + dt_1.strftime("%Y-%m-%d") + ".csv"

# read pnl summary into dataframe
df_pnl = pd.read_csv(path+filename)


# create new sliced dataframe for new trades only
df_tradeblotter =  \
    df_pnl.loc[df_pnl['explanation'] == 'New Position',
               ['id', 'primary_product_code', 'trade_date',
                   'book', 'counterparty', 'right', 'strike', 'quantity',
                   'contract_start', 'price', 'url']]

# load entire trade blotter from molecule
# df_tradeblotter =  \
#    df_pnl.loc[:,
#               ['id', 'primary_product_code', 'trade_date',
#                   'book', 'counterparty', 'right', 'strike', 'quantity',
#                   'contract_start', 'price', 'url']]

# create a unique id for multiple legged trades.
# Required so that a primary key set can be created for mysql database table
legid = [0] * df_tradeblotter.shape[0]
legid[0] = 1
for i in range(1, df_tradeblotter.shape[0]):
    if (df_tradeblotter['id'].iloc[i] == df_tradeblotter['id'].iloc[i - 1]):
        legid[i] = legid[i - 1] + 1
    else:
        legid[i] = 1

# append new id to trade_blotter
df_tradeblotter['legid'] = pd.Series(legid, index=df_tradeblotter.index)

# inplace replace the primary_product_code from string to integer
# through tickermappings dictionary
# this is done because the database is setup to take integers as 'foreign key'
# into tickermappings table
df_tradeblotter['primary_product_code'].\
    replace(dict_tickermappings['tickerId'], inplace=True)

# change call/put to C/P
df_tradeblotter['right'].replace(dict_cp, inplace=True)


# rename columns to match database
df_tradeblotter.columns = ['TradeId', 'Ticker', 'TradeDate',
                           'FCMAccount', 'CounterParty', 'CP', 'Strike',
                           'Quantity', 'ContractStart', 'TradePrice',
                           'MoleculeURL', 'LegId']


# insert data into trade_blotter
df_tradeblotter.to_sql('trade_blotter',
                       con=cnx, if_exists='append', index=False)

# Close database connection
cnx.close()
