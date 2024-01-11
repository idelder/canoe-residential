"""
Builds residential sector database
Written by Ian David Elder for the CANOE model
"""

import pandas as pd
import sqlite3
import os
import utils
from scipy.special import gamma
from setup import config

this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
input_files = this_dir + 'input_files/'
schema_file = this_dir + "canoe_schema.sql"
database_file = this_dir + "residential.sqlite"

# Check if database exists or needs to be built
build_db = not os.path.exists(database_file)

# Connect to the new database file
conn = sqlite3.connect(database_file)
curs = conn.cursor() # Cursor object interacts with the sqlite db

# Instantiate the database if it doesn't exist
if build_db: curs.executescript(open(schema_file, 'r').read())



"""
##############################################################
    AEO data (future vintages)
##############################################################
"""

##############################################################
# Lifetime
##############################################################

config.lifetimes = {}

## AEO technologies (future vintages)
note = 'Average of Weibull distribution'
aeo_ref = config.params['aeo_reference']
aeo_year = config.params['aeo_data_year']

for tech in config.aeo_techs.index:

    aeo_tech = config.aeo_techs.loc[tech, 'aeo_tech']
    end_use_ids = config.aeo_techs.loc[tech, 'end_use_ids'].split('+')

    # Get lifetime from mean of weibull distribution
    weibull_k = config.aeo_res_class.loc[config.aeo_res_class['End Use'] == int(end_use_ids[0])].loc[aeo_tech, 'Weibull K']
    weibull_l = config.aeo_res_class.loc[config.aeo_res_class['End Use'] == int(end_use_ids[0])].loc[aeo_tech, 'Weibull λ']

    lifetime = round(weibull_l * gamma(1 + 1/weibull_k)) # mean of weibull distribution

    # Add feasible vintages to config dictionary
    config.tech_vints[tech] = config.model_periods

    for region in config.regions.index:
        curs.execute(f"""REPLACE INTO
                    LifetimeTech(regions, tech, life, life_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', '{tech}', {lifetime}, '{note}',
                    '{aeo_ref}', {2023}, 1, 1, 1, 1, 1, 1)""")


## NRCan technologies (existing vintages)
nrcan_ref = config.params['nrcan_reference']
nrcan_year = config.params['nrcan_data_year']

for tech in config.nrcan_techs.index:

    # Get equivalent future tech to this existing tech to pull AEO data
    aeo_tech = config.nrcan_techs.loc[tech, 'aeo_tech']
    equiv_tech = config.aeo_techs.loc[config.aeo_techs['aeo_tech']==aeo_tech].index.values[0]
    end_use_ids = config.aeo_techs.loc[equiv_tech, 'end_use_ids'].split('+')
    
    # Get lifetime from mean of weibull distribution
    weibull_k = config.aeo_res_class.loc[config.aeo_res_class['End Use'] == int(end_use_ids[0])].loc[aeo_tech, 'Weibull K']
    weibull_l = config.aeo_res_class.loc[config.aeo_res_class['End Use'] == int(end_use_ids[0])].loc[aeo_tech, 'Weibull λ']

    lifetime = round(weibull_l * gamma(1 + 1/weibull_k)) # mean of weibull distribution

    # Add feasible vintages to config dictionary
    config.tech_vints[tech] = utils.feasible_vintages(config.model_periods[0], config.params['period_step'], lifetime)

    note = f"Assumed same as {equiv_tech}"

    for region in config.regions.index:
        curs.execute(f"""REPLACE INTO
                    LifetimeTech(regions, tech, life, life_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', '{tech}', {lifetime}, '{note}',
                    '{aeo_ref}', {nrcan_year}, 1, 1, 1, 1, 1, 1)""")



"""
##############################################################
    Space Heating
##############################################################
"""

conn.commit()
conn.close()