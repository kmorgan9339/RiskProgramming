# -*- coding: utf-8 -*-
"""
Created on Wed Jul 26 12:54:56 2017

@author: KevinMorgan
"""

#%% ++++++++ Import Libraries ++++++++++++++
from __future__ import print_function
from datetime import timedelta
import numpy as np
import QuantLib as ql


#%% +++++++ datediff +++++++++++++++++++++++


def date_by_subtracting_business_days(from_date, subtract_days, holidays):
    business_days_to_subtract = subtract_days
    current_date = from_date
    while business_days_to_subtract > 0:
        current_date -= timedelta(days=1)
        weekday = current_date.weekday()
        if weekday >= 5:
            continue
        if current_date in holidays:
            continue
        business_days_to_subtract -= 1
    return current_date

#%% +++++++++ black76 from QuantLib +++++++++++++++


def Black76(pc, underlying, strike, impvol, ir, calc_date, expiry_date, param):
    # calendar = ql.UnitedStates()
    # bussiness_convention = ql.ModifiedFollowing
    # settlement_days = 2
    if (calc_date < expiry_date):
        day_count = ql.ActualActual()
        interest_rate = ir
        yield_curve = ql.FlatForward(calc_date,
                                     interest_rate,
                                     day_count,
                                     ql.Compounded,
                                     ql.Continuous)
        discount = yield_curve.discount(expiry_date)
        T = day_count.yearFraction(calc_date, expiry_date)
        stdev = impvol * np.sqrt(T)
        if (pc.lower() == 'c'):
            strikepayoff = ql.PlainVanillaPayoff(ql.Option.Call, strike)
        else:
            strikepayoff = ql.PlainVanillaPayoff(ql.Option.Put, strike)
        black = ql.BlackCalculator(strikepayoff, underlying, stdev, discount)
        if (param.lower() == 'premium'):
            return black.value()
        elif (param.lower() == 'delta'):
            return black.delta(underlying)
        elif (param.lower() == 'gamma'):
            return black.gamma(underlying)
        elif (param.lower() == 'theta'):
            return black.theta(underlying, T)
        elif (param.lower() == 'vega'):
            return black.vega(T)
        else:
            return 'no answer for you!'
    else:
        return 0.0




# prem = Black76('c', 2.915, 3.25, 0.32965, 0.0178,
#               ql.Date(26, 7, 2017), ql.Date(28, 8, 2017), 'premium')
# delta = Black76('c', 2.915, 3.25, 0.32965, 0.0178,
#               ql.Date(26, 7, 2017), ql.Date(28, 8, 2017), 'delta')
# gamma = Black76('c', 2.915, 3.25, 0.32965, 0.0178,
#               ql.Date(26, 7, 2017), ql.Date(28, 8, 2017), 'gamma')
# vega = Black76('c', 2.915, 3.25, 0.32965, 0.0178,
#               ql.Date(26, 7, 2017), ql.Date(28, 8, 2017), 'vega')
#
# print('Premium ', prem)
# print('Delta ', delta)
# print('gamma ', gamma)
# print('Vega ', vega)

# t = ql.Date(26, 7, 2017)
