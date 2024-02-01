"""
Aggregates data for residential water heating
Written by Ian David Elder for the CANOE model
"""

import utils
import pandas as pd
import os
from scipy.special import gamma
import numpy as np
import sqlite3
from matplotlib import pyplot as pp
from setup import config

# Shortens lines a bit
base_year = config.params['base_year']
nrcan_ref = config.params['nrcan_reference']
aeo_year = config.params['aeo_data_year']
aeo_ref = config.params['aeo_reference']
statcan_year = config.params['statcan_data_year']
statcan_ref = config.params['statcan_reference']
fuel_commodities = config.fuel_commodities
nrcan_techs = config.nrcan_techs
aeo_techs = config.aeo_techs
aeo_res_class = config.aeo_res_class
aeo_res_equip = config.aeo_res_equip



def aggregate(region):

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db



    """
    ##############################################################
        Efficiency of existing stock
    ##############################################################
    """

    out_comm = c2a = config.end_use_demands.loc['water heating', 'comm']
    stock_effs = dict() # track efficiencies by nrcan stock

    for tech, row in config.nrcan_techs.iterrows():
        if row['end_use'] != 'water heating': continue

        # TAKE FROM AEO CLASS
        note = "(PJ/PJ)"

        # Input commodity
        in_comm = fuel_commodities.loc[row.loc['fuels'], 'comm']

        # Taking efficiency from base efficiency of AEO class - not great but it'll do
        eff = aeo_res_class.loc[row['aeo_class'], 'Base Efficiency']
        stock_effs[row['nrcan_stocks']] = eff

        # Write single fuel efficiencies to database
        for vint in config.tech_vints[tech]:
            if vint + config.lifetimes[tech] <= config.model_periods[0]: continue

            curs.execute(f"""REPLACE INTO
                Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES('{region}', '{in_comm}', '{tech}', {vint}, '{out_comm}', {eff}, '{note}',
                '{aeo_ref}', {aeo_year}, 3, 1, 1, 1, 3, 3)""")
            


    """
    ##############################################################
        Demand
    ##############################################################
    """

    note = (f"Sum of {base_year} secondary energy multiplied by efficiency per technology (NRCan, {base_year}). "
            f"Indexed to projected population (Statcan, {statcan_year})")
    reference = f"{nrcan_ref}; {statcan_ref}"

    # Table 10: Water Heating Secondary Energy Use and GHG Emissions by Energy Source
    t10_sec = utils.get_compr_db(region, 10, 3, 7)

    # Activity (PJ output) is secondary energy times efficiency, and demand is sum of activity
    activity = t10_sec.copy()
    for nrcan_stock, row in activity.iterrows():
        row *= stock_effs[nrcan_stock]

    # Index demand to population growth
    pop = config.populations[region]
    dem = activity[base_year].sum() * pop / pop.loc[base_year]

    # Write to database
    for period in config.model_periods:
        curs.execute(f"""REPLACE INTO
                    Demand(regions, periods, demand_comm, demand, demand_units, demand_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', {period}, '{out_comm}', {float(dem.loc[period])}, '(PJ)', '{note}',
                    '{reference}', {base_year}, 2, 1, 1, {utils.dq_time(period, base_year)}, 1, 3)""")

    

    """
    ##############################################################
        Existing Capacity and Annual Capacity Factor
    ##############################################################
    """

    # Table 28: Water Heater Stock by Building Type and Energy Source
    t28_stk = utils.get_compr_db(region, 28, 15, 20)/1000 # Munit

    # Notes for database
    note = f"{base_year} stock (NRCan, {base_year}) indexed to population (Statcan, {statcan_year}) and distributed evenly over feasible past vintages."
    reference = nrcan_ref

    # Get existing capacities from NRCan stock and distribute over past vintages
    for tech, row in nrcan_techs.iterrows():
        if row['end_use'] != 'water heating': continue

        nrcan_stock = row.loc['nrcan_stocks']

        ## Existing capacity
        # Get existing capacity (stock) from nrcan and index to population growth
        existing_cap = t28_stk.loc[nrcan_stock, base_year]
        
        # Index to population and distribute existing capacities evenly over feasible vintages
        vints = config.tech_vints[tech]

        # Write existing capacities to database
        for vint in vints:
            if vint + config.lifetimes[tech] <= config.model_periods[0]: continue

            curs.execute(f"""REPLACE INTO
                        ExistingCapacity(regions, tech, vintage, exist_cap, exist_cap_units, exist_cap_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {vint}, {existing_cap / len(vints)}, '(Munit)', '{note}',
                        '{reference}', {base_year}, 1, 1, 1, {utils.dq_time(config.model_periods[0], base_year)}, 1, 1)""")
        

        ## Annual capacity factor for NRCan existing stock
        # (for new stock pulled in all sectors post processing)
        max_note = (f"Annual utilisation of units. (annual secondary energy consumption * efficiency) / (c2a * existing stock) (NRCan, {base_year})")
        min_note = "99% of MaxACF. " + max_note

        act = activity[base_year].loc[nrcan_stock] # annual PJ output
        c2a = config.end_use_demands.loc['water heating', 'c2a']

        # Annual capacity factor is actual annual activity divided by max possible annual activity from arbitrary c2a
        acf = act / (existing_cap * c2a)

        for period in config.model_periods:
            if max(vints) + config.lifetimes[tech] <= period: continue
            
            curs.execute(f"""REPLACE INTO
                            MinAnnualCapacityFactor(regions, periods, tech, output_comm, min_acf, min_acf_notes,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{tech}', '{out_comm}', {acf*0.99}, '{min_note}',
                            '{reference}', {base_year}, 1, 1, 1, {utils.dq_time(period, base_year)}, 1, 1)""")
            curs.execute(f"""REPLACE INTO
                            MaxAnnualCapacityFactor(regions, periods, tech, output_comm, max_acf, max_acf_notes,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{tech}', '{out_comm}', {acf}, '{max_note}',
                            '{reference}', {base_year}, 1, 1, 1, {utils.dq_time(period, base_year)}, 1, 1)""")


    conn.commit()
    conn.close()