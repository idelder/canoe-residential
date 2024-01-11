"""
Aggregates data for residential space heating
Written by Ian David Elder for the CANOE model
"""

import utils
import pandas as pd
import os
import numpy as np
import sqlite3
from setup import config

data_year = 2020
region = 'ON'

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

# Get technologies config table
technologies = config.technologies.loc[config.technologies['subsector'].str.contains('space heating')]



"""
##############################################################
    Demand
##############################################################
"""

# Table 8: Space Heating Secondary Energy Use by System Type
t8 = utils.get_data(f"https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/data_e/downloads/comprehensive/Excel/{data_year}/res_{region.lower()}_e_8.xls", skiprows=10)
t8 = t8.loc[3:17].rename(columns={'Unnamed: 1':'tech'}).drop("Unnamed: 0", axis=1).set_index('tech').dropna()

# Table 26: Heating System Stock Efficiencies
t26 = utils.get_data(f"https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/data_e/downloads/comprehensive/Excel/{data_year}/res_{region.lower()}_e_26.xls", skiprows=10)
t26 = t26.loc[2:27].rename(columns={'Unnamed: 1':'tech'}).drop("Unnamed: 0", axis=1).set_index('tech').dropna()/100

# Multiply secondary energy by efficiency to get output heating energy
# Dual fuel systems make this a little painful
dem_pj = t8.copy() # output primary energy (demand) by technology
tracker = dict() # tracking multiples of the same fuel type
for tech in t8.index:

    fuel = tech
    if "/" in fuel: fuel = fuel.split("/")[0]

    eff = t26.loc[fuel, data_year]

    # To handle multple techs of the same fuel, take efficiencies from the table in order
    if fuel not in tracker.keys(): tracker[fuel] = 0
    if type(eff) is pd.Series:
        eff = eff[tracker[fuel]]
        tracker[fuel] += 1

    # Demanded primary energy is secondary input energy times efficiency
    # Dual fuel systems are assumed to consume the first listed fuel for simplicity
    dem_pj.loc[tech, data_year] *= eff

# Index demand to population growth
pop = pd.read_excel(this_dir + "input_files/population.xlsx", sheet_name=region, index_col=0)
dem_pj = dem_pj[data_year].sum() * pop / pop.loc[data_year]

# Write to database
note = f"Sum of {data_year} secondary energy multiplied by efficiency per technology. Dual fuel boilers taken to consume only first listed fuel in this calculation. (NRCan, {data_year}) and indexed to projected Ontario population (Statcan, 2022)"
reference = f"Comprehensive Energy Use Database. Government of Canada, Natural Resources Canada. ({data_year}) https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm; Statistics Canada, (2022, August 22). Projected population, by projection scenario, age and sex, as of July 1. https://www150.statcan.gc.ca/t1/tbl1/en/cv.action?pid=1710005701"

for period in config.model_periods:
    curs.execute(f"""REPLACE INTO
                 Demand(regions, periods, demand_comm, demand, demand_units, demand_notes,
                 reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                 VALUES('{region}', {period}, '{config.params['demand_commodities']['space_heating']}', {float(dem_pj.loc[period])}, 'PJ', '{note}',
                 '{reference}', {data_year}, 1, 1, 1, {utils.dq_time(period, data_year)}, 1, 1)""")



"""
##############################################################
    Lifetime
##############################################################
"""



"""
##############################################################
    Efficiency
##############################################################
"""

note = '(PJ/PJ)'
reference = f"Comprehensive Energy Use Database. Government of Canada, Natural Resources Canada. ({data_year}) https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm"

tracker = dict() # tracking multiples of the same nrcan fuel type
for tech in technologies.index:

    ##############################################################
    #   Existing vintages
    ##############################################################
    
    # A technology with existing stock in the comprehensive database
    nrcan_tech = technologies.loc[tech, 'nrcan_stock']
    if not pd.isna(nrcan_tech):

        ## Dual fuel boiler, needs two efficiencies
        # These are uncommon so only including existing vintages
        if "/" in nrcan_tech:
            fuels = nrcan_tech.split("/")

            for fuel in fuels:
                fuel = fuel.replace('Electric','Electricity')
                eff = t26.loc[fuel, data_year]

                # To handle multple techs of the same fuel, take efficiencies from the table in order, skipping one for wood here
                if fuel not in tracker.keys(): tracker[fuel] = 1 if fuel == 'Wood' else 0
                if type(eff) is pd.Series: # multiples exist
                    eff = eff[tracker[fuel]] # take the next in the list
                    tracker[fuel] += 1 # move up the tracker

                # Get model commodities
                in_comm = technologies.loc[tech, 'input_comm']
                out_comm = config.params['demand_commodities']['space_heating']

                # TODO need lifetimes
                lifetime = 21 #eg
                # Get feasible existing vintages
                # TODO use build a tech vints table
                vints = utils.feasible_vintages(config.model_periods[0], config.params['period_step'], lifetime)

                for vint in vints:
                    # Write dual fuel efficiencies to database
                    curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{in_comm}', '{tech}', {vint}, '{out_comm}', {eff}, '{note}',
                        '{reference}', {data_year}, 1, 1, 1, 1, 1, 1)""")

        ## Single fuel heating technology     
        else:
            eff = t26.loc[nrcan_tech, data_year]
            if type(eff) is pd.Series: eff = eff.iloc[0]
            
            # Get model commodities
            in_comm = technologies.loc[tech, 'input_comm']
            out_comm = config.params['demand_commodities']['space_heating']

            # TODO need lifetimes
            lifetime = 21 #eg
            # Get feasible existing vintages
            # TODO use build a tech vints table
            vints = utils.feasible_vintages(config.model_periods[0], config.params['period_step'], lifetime)

            for vint in vints:
                # Write dual fuel efficiencies to database
                curs.execute(f"""REPLACE INTO
                    Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', '{in_comm}', '{tech}', {vint}, '{out_comm}', {eff}, '{note}',
                    '{reference}', {data_year}, 1, 1, 1, 1, 1, 1)""")
        
    ##############################################################
    #   Future vintages
    ##############################################################
    
    vints = config.model_periods

    # Get efficiencies from AEO data
    # Write efficiencies to database
    

"""
##############################################################
    Existing Capacity
##############################################################
"""

# Table 21: Heating System Stock by Building Type and Heating System Type
t21 = utils.get_data(f"https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/data_e/downloads/comprehensive/Excel/{data_year}/res_{region.lower()}_e_21.xls", skiprows=10)
t21 = t21.loc[16:30].rename(columns={'Unnamed: 1':'tech'}).drop("Unnamed: 0", axis=1).set_index('tech').dropna()*1000

# Notes for database
note = f"{data_year} stock distributed evenly across feasible 5y vintages for that years stock (NRCan, {data_year})"
reference = f"Comprehensive Energy Use Database. Government of Canada, Natural Resources Canada. ({data_year}) https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/menus/trends/comprehensive_tables/list.cfm"

# Get existing capacities from NRCan stock and distribute over past vintages
for tech, row in technologies.iterrows():
    if pd.isna(row.loc['nrcan_stock']): continue # no existing stock from NRCan

    existing_cap = t21.loc[row.loc['nrcan_stock'], data_year] / 1E6 # Munit
    
    # Distribute existing capacities evenly over feasible past vintages, relative to data year
    lifetime = 21 #eg TODO get actual lifetimes from aeo data
    vints = utils.feasible_vintages(period=data_year, vint_interval=config.params['period_step'], lifetime=lifetime)
    existing_cap /= len(vints)

    for vint in vints:
        if vint + lifetime <= config.model_periods[0]: continue # reject infeasible model vintages

        # Write to database
        curs.execute(f"""REPLACE INTO
                    ExistingCapacity(regions, tech, vintage, exist_cap, exist_cap_units, exist_cap_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', '{tech}', {vint}, {existing_cap}, 'Munit', '{note}',
                    '{reference}', {data_year}, 1, 1, 1, {utils.dq_time(period, data_year)}, 1, 1)""")

conn.commit()
conn.close()