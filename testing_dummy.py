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
from datetime import datetime

# Connect to the new database file
conn = sqlite3.connect(config.database_file)
curs = conn.cursor() # Cursor object interacts with the sqlite db

for region in config.regions:
    for tech in config.all_techs:
        
        vints = config.tech_vints(tech)
        life = curs.execute(f"SELECT life FROM LifetimeTech WHERE regions == '{region}' AND tech == '{tech}'").fetchone()[0]
        acf = curs.execute(f"SELECT max_acf FROM MaxAnnualCapacityFactor WHERE regions == '{region}' AND tech == '{tech}'").fetchone()[0]
        c2a = curs.execute(f"SELECT c2a FROM CapacityToActivity WHERE regions == '{region}' AND tech == '{tech}'").fetchone()[0]

        annual_act = c2a * acf

        for vint in vints:
            cost_invest = curs.execute(f"SELECT data_cost_invest FROM CostInvest WHERE regions == '{region}' AND tech == '{tech}' AND vintage == {vint}").fetchone()[0]
            i = config.params['global_discount_rate']
            annuity = cost_invest * i * (1+i)^life / ((1+i)^life - 1)

            cost_fixed = curs.execute(f"SELECT data_cost_fixed FROM CostFixed WHERE  regions == '{region}' AND tech == '{tech}' and vintage == {vint}").fetchone()[0]

            lcoa = (cost_fixed + cost_invest) / annuity

            for period in config.model_periods:
                if vint > period or vint + life <= period: continue

                curs.execute(f"""REPLACE INTO
                             CostVariable(regions, periods, tech, vintage, data_cost_variable, data_cost_year, data_curr)
                             VALUES('{region}', {period}, '{tech}', {vint}, {lcoa}, {2022}, '{"USD"}')""")