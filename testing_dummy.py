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

days_in_months = [31,28,31,30,31,30,31,31,30,31,30,31]

df_wth = utils.get_data('https://www.ncei.noaa.gov/data/global-hourly/access/2018/72537514822.csv', index_col=1)
df_wth = df_wth.loc[df_wth.index.str.contains('53')]
temp = list(range(8760))

i=0
for m in range(12):
    for d in range(days_in_months[m]):
        for h in range(24):
            ms = f"0{m+1}" if m+1 < 10 else str(m+1)
            ds = f"0{d+1}" if d+1 < 10 else str(d+1)
            hs = f"0{h}" if h < 10 else str(h)
            idx = f"2018-{ms}-{ds}T{hs}:53:00"
            if idx not in df_wth.index: tmp = 0
            else: tmp = float(df_wth.loc[idx, 'TMP'].split(',')[0])/10
            if tmp > 900: tmp = 0
            temp[i]=tmp
            i+=1

dsd = list(range(8760))
for el in curs.execute("SELECT time_of_day_name, dds FROM DemandSpecificDistribution WHERE regions == 'ON' AND demand_name = 'D_R_SPH'").fetchall():
    dsd[int(el[0])] = float(el[1])

df_temp = pd.DataFrame(index=temp, data=dsd)

avg_ts = list(range(-30,40))
for i in range(len(avg_ts)):
    avg_t = df_temp.loc[(df_temp.index>avg_ts[i]) & (df_temp.index<=avg_ts[i]+1)].mean()
    avg_ts[i] = avg_t

pp.plot(range(-30,40), avg_ts, 'b.')
pp.show()

conn.close()