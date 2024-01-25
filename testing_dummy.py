"""
For testing code snippets
"""

import pandas as pd
from setup import config
import requests
import utils
import os
import urllib.request
import zipfile
import time
import sqlite3
import numpy as np
from matplotlib import pyplot as pp

conn = sqlite3.connect('residential.sqlite')
curs = conn.cursor()

# Get these fron regions config
state='MI'
us_station = '71538099999'
region='ON'
ca_station = '6158359'

# Days in each month of the year. Need because of dodgy index
days_in_months = [31,28,31,30,31,30,31,31,30,31,30,31]

# Weather data from the US
df_us_wth = utils.get_data(config.params['weather']['us_url'].replace('<st>',us_station), index_col=1)
df_us_wth = df_us_wth.loc[df_us_wth.index.str.contains('53')]
df_us = pd.DataFrame(index=range(8760))
temp = list(range(8760))
dew = list(range(8760))

# Mapping 2018 DSD from Resstock to 2018 us weather data
i=0
for m in range(12):
    for d in range(days_in_months[m]):
        for h in range(24):
            ms = f"0{m+1}" if m+1 < 10 else str(m+1)
            ds = f"0{d+1}" if d+1 < 10 else str(d+1)
            hs = f"0{h}" if h < 10 else str(h)
            idx = f"2018-{ms}-{ds}T{hs}:53:00"

            for val in ['TMP','DEW']:
                if idx not in df_us_wth.index: v = pd.NA
                else:
                    v = float(df_us_wth.loc[idx, val].split(',')[0])/10
                    if v > 50: v = pd.NA
                df_us.loc[i, val] = v

            i+=1

dsd_us = list(range(8760))
for el in curs.execute("SELECT time_of_day_name, dds FROM DemandSpecificDistribution WHERE regions == 'ON' AND demand_name = 'D_R_SPH'").fetchall():
    dsd_us[int(el[0])] = float(el[1]) # Data not in order

# Fill in temperature gaps by chronological linear interpolation
df_us['dsd'] = dsd_us
df_us.interpolate(method='linear', axis='columns', inplace=True)

# Diurnal factor on DSD
dsd_24h = [sum(dsd_us[h:364*24+h:24]) for h in range(24)]
dsd_24h /= np.mean(dsd_24h)

# Canadian weather data
df_ca = utils.get_data(config.params['weather']['canada_url'].replace('<st>', ca_station).replace('<r>', region), encoding='unicode_escape')

# Take US DSD where temp and dew temp are +-1 matching, then apply diurnal factor
for idx, row in df_ca.iterrows():
    h = idx % 24
    dsd = df_us.loc[(row['Temp (°C)'] < df_us['TMP']+1) &
                         (row['Temp (°C)'] > df_us['TMP']-1) &
                         (row['Dew Point Temp (°C)'] > df_us['DEW']-1) &
                         (row['Dew Point Temp (°C)'] < df_us['DEW']+1)]['dsd'].mean()
    df_ca.loc[idx,'dsd'] = dsd * dsd_24h[h]

# Fill in dsd gaps by chronological linear interpolation then renormalise
df_ca['dsd'].interpolate(method='linear', inplace=True)
df_ca['dsd'] /= df_ca['dsd'].sum()

pp.figure()
pp.plot(df_ca['dsd'])
pp.title("Ontario")
pp.figure()
pp.plot(df_us['dsd'])
pp.title("Michigan")
pp.show()

conn.close()