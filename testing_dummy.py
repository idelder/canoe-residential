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
import all_subsectors

populations = dict()

df_exs = utils.get_statcan_table(17100009, usecols=[0,1,9])
df_exs = df_exs.loc[df_exs['REF_DATE'].str.contains('-01')]
df_exs['REF_DATE'] = df_exs['REF_DATE'].str.removesuffix("-01")
df_proj = utils.get_statcan_table(17100057, usecols=[0,1,3,4,5,12])
df_proj['VALUE'] *= 1000
df_proj = df_proj.loc[
    (df_proj['Projection scenario'] == 'Projection scenario M1: medium-growth') & 
    (df_proj['Sex'] == 'Both sexes') &
    (df_proj['Age group'] == 'All ages')]

for region, row in config.regions.iterrows():

    exs = df_exs.loc[df_exs['GEO'].str.upper() == row['description'].upper()].dropna()
    proj = df_proj.loc[(df_proj['GEO'].str.upper() == row['description'].upper()) &
                       (df_proj['REF_DATE'] > int(exs['REF_DATE'].values[-1]))].dropna()
    ca = df_proj.loc[(df_proj['GEO'].str.upper() == 'CANADA') & 
                     (df_proj['REF_DATE'] >= int(proj['REF_DATE'].values[-1]))].dropna()
    ca['VALUE'] = ca['VALUE'].iloc[1::] * proj['VALUE'].values[-1] / ca['VALUE'].values[0]
    ca.dropna(inplace=True)

    data = [*exs['VALUE'].to_list(), *proj['VALUE'].to_list(), *ca['VALUE'].to_list()]

    pop = pd.DataFrame(index = range(int(exs['REF_DATE'].values[0]), int(ca['REF_DATE'].values[-1]+1)), data = data, dtype=int, columns=['population'])
    pop.index.rename('year', inplace=True)

    populations[region] = pop

print(populations['ON'].loc[range(2000,2050)])
print(config.populations['ON'])