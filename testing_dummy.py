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

#state = 'mo'
df = utils.get_data(f"https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2022/resstock_amy2018_release_1.1/timeseries_aggregates/by_state/upgrade=10/state={state.upper()}/up10-{state.lower()}-mobile_home.csv")

hb_uec = utils.get_data(f"https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/data_e/downloads/handbook/Excel/{2020}/res_00_16_e.xls", skiprows=7)
hb_uec = hb_uec.drop('Unnamed: 0', axis=1).set_index('Unnamed: 1').dropna()
utils.clean_index(hb_uec)
hb_uec_elc = hb_uec.iloc[0:6]
hb_uec_ng = hb_uec.iloc[6:8]
print(hb_uec_elc)
print(hb_uec_ng)