"""
Aggregates data for residential space heating
Written by Ian David Elder for the CANOE model
"""

import utils
import pandas as pd
import os
import shutil
import sqlite3
from setup import config

data_year = 2020
region = 'ON'

this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
schema_file = this_dir + "canoe_schema.sql"
database_file = this_dir + "residential.sqlite"

# Check if database exists or needs to be built
build_db = not os.path.exists(database_file)

# Connect to the new database file
conn = sqlite3.connect(database_file)
curs = conn.cursor() # Cursor object interacts with the sqlite db

# Build the database if it doesn't exist
if build_db: curs.executescript(open(schema_file, 'r').read())



"""
##############################################################
    Demand
##############################################################
"""

# Table 16: Single Detached and Single Attached Housing Stock by Vintage
t16 = utils.get_data(f"https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/data_e/downloads/comprehensive/Excel/{data_year}/res_{region.lower()}_e_16.xls", skiprows=10)
stock_sd = t16.loc[3:12].rename(columns={'Unnamed: 1':'vintage'}).drop("Unnamed: 0", axis=1).set_index('vintage')*1000
stock_sa = t16.loc[29:38].rename(columns={'Unnamed: 1':'vintage'}).drop("Unnamed: 0", axis=1).set_index('vintage')*1000

# Table 17: Apartments and Mobile Homes Housing Stock by Vintage
t17 = utils.get_data(f"https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/data_e/downloads/comprehensive/Excel/{data_year}/res_{region.lower()}_e_17.xls", skiprows=10)
stock_apt = t17.loc[3:12].rename(columns={'Unnamed: 1':'vintage'}).drop("Unnamed: 0", axis=1).set_index('vintage')*1000
stock_mob = t17.loc[29:38].rename(columns={'Unnamed: 1':'vintage'}).drop("Unnamed: 0", axis=1).set_index('vintage')*1000

# Table 32: Gross Output Thermal Requirements per Household by Building Type and Vintage
t32 = utils.get_data(f"https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/data_e/downloads/comprehensive/Excel/{data_year}/res_{region.lower()}_e_32.xls", skiprows=10)
pj_sd = t32.loc[2:11].rename(columns={'Unnamed: 1':'vintage'}).drop("Unnamed: 0", axis=1).set_index('vintage')/1E6
pj_sa = t32.loc[15:24].rename(columns={'Unnamed: 1':'vintage'}).drop("Unnamed: 0", axis=1).set_index('vintage')/1E6
pj_apt = t32.loc[28:37].rename(columns={'Unnamed: 1':'vintage'}).drop("Unnamed: 0", axis=1).set_index('vintage')/1E6
pj_mob = t32.loc[41:50].rename(columns={'Unnamed: 1':'vintage'}).drop("Unnamed: 0", axis=1).set_index('vintage')/1E6

# Sum up demand across all housing types
pj_tot = stock_sd*pj_sd + stock_sa*pj_sa + stock_apt*pj_apt + stock_mob*pj_mob

# Index demand to population growth
pop = pd.read_excel(this_dir + "input_files/population.xlsx", sheet_name=region, index_col=0)
dem_pj = pj_tot[data_year].sum() * pop / pop.loc[data_year]

# Write to database
note = "2020 gross thermal output requirements matched to 2020 housing stock (NRCan) and indexed to projected Ontario population (Statcan)"
reference = f"Comprehensive Energy Use Database. Government of Canada, Natural Resources Canada. ({data_year}) https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm; Statistics Canada, (2022, August 22). Projected population, by projection scenario, age and sex, as of July 1. https://www150.statcan.gc.ca/t1/tbl1/en/cv.action?pid=1710005701"

for period in config.model_periods:
    curs.execute(f"""REPLACE INTO
                 Demand(regions, periods, demand_comm, demand, demand_units,
                 demand_notes, reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                 VALUES('{region.lower()}', {period}, '{config.params['demand_commodities']['space_heating']}', {dem_pj.loc[period].values[0]}, 'PJ',
                 '{note}', '{reference}', {data_year}, 1, 1, 1, {utils.dq_time(period, data_year)}, 1, 1)""")



"""
##############################################################
    Existing Capacity
##############################################################
"""

# 2020 stock (NRCan, 2023)
# Capacity distributed over feasible vintages to preserve average age equals half of lifetime.

# Table 21: Heating System Stock by Building Type and Heating System Type
t21 = utils.get_data(f"https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/data_e/downloads/comprehensive/Excel/{data_year}/res_{region.lower()}_e_21.xls", skiprows=10)
t21 = t21.loc[16:30].rename(columns={'Unnamed: 1':'tech'}).drop("Unnamed: 0", axis=1).set_index('tech').dropna()*1000

print(t21)

conn.commit()
conn.close()