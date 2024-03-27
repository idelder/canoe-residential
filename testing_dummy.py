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
from datetime import datetime
from sklearn.metrics import mean_squared_error, r2_score

df = pd.read_csv('on_sph.csv', index_col=0)

me = df['me']
mi = df['mi']
ninja = df['ninja'].shift(-22)

me = me.loc[ninja.notna()]
ninja = ninja.loc[ninja.notna()]

pp.figure()
pp.plot(me,'b-')
pp.plot(ninja,'r-')
#pp.plot(mi)

mse = mean_squared_error(me, ninja)
r2 = r2_score(me, ninja)

print(mse, r2)

pp.show()