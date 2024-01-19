"""
Aggregates data for residential space cooling
Written by Ian David Elder for the CANOE model
"""

import utils
import pandas as pd
import os
from scipy.special import gamma
import numpy as np
import sqlite3
from setup import config

this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
input_files = this_dir + 'input_files/'
schema_file = this_dir + "canoe_schema.sql"
database_file = this_dir + "residential.sqlite"

# Shortens lines a bit
nrcan_year = config.params['nrcan_data_year']
nrcan_ref = config.params['nrcan_reference']
statcan_year = config.params['statcan_data_year']
statcan_ref = config.params['statcan_reference']
fuel_commodities = config.fuel_commodities
nrcan_techs = config.nrcan_techs


def aggregate(region):

    # Connect to the new database file
    conn = sqlite3.connect(database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    """
    ##############################################################
        Demand
    ##############################################################
    """

    note = (f"Sum of {nrcan_year} secondary energy multiplied by efficiency per technology (NRCan, {nrcan_year}). "
            f"Indexed to projected population (Statcan, {statcan_year})")
    reference = f"{nrcan_ref}; {statcan_ref}"

    # Table 4: Space Cooling Secondary Energy Use and GHG Emissions by Cooling System Type
    t4 = utils.get_data(utils.compr_db_url(region, 4), skiprows=10)
    t4_sec = t4.loc[3:4].rename(columns={'Unnamed: 1':'tech'}).drop("Unnamed: 0", axis=1).set_index('tech').dropna()
    utils.clean_index(t4_sec)

    # Table 27: Cooling System Stock by Type, New Unit Efficiencies, Stock Efficiencies and Unit Capacity Ratio
    t27 = utils.get_data(utils.compr_db_url(region, 27), skiprows=10)
    t27_stk_eff = t27.loc[15:16].rename(columns={'Unnamed: 1':'tech'}).drop("Unnamed: 0", axis=1).set_index('tech').dropna()
    t27_stk_eff.index=t4_sec.index
    t27_stk_eff *= config.params['conversion_factors']['efficiency']['EER']

    # Activity (PJ output) is secondary energy times efficiency, and demand is sum of activity
    activity = t4_sec.values * t27_stk_eff.values
    activity = pd.DataFrame(data=activity, columns=t4_sec.columns, index=t4_sec.index)

    # Index demand to population growth
    pop = config.populations[region]
    dem = activity[nrcan_year].sum() * pop / pop.loc[nrcan_year]

    # Write to database
    for period in config.model_periods:
        curs.execute(f"""REPLACE INTO
                    Demand(regions, periods, demand_comm, demand, demand_units, demand_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', {period}, '{config.params['demand_commodities']['space cooling']}', {float(dem.loc[period])}, 'PJ', '{note}',
                    '{reference}', {nrcan_year}, 1, 1, 1, {utils.dq_time(period, nrcan_year)}, 1, 1)""")



    """
    ##############################################################
        Efficiency of existing stock
    ##############################################################
    """

    t27_new_eff = t27.loc[15:16].rename(columns={'Unnamed: 1':'tech'}).drop("Unnamed: 0", axis=1).set_index('tech').dropna()
    t27_new_eff.index=t4_sec.index
    t27_new_eff *= config.params['conversion_factors']['efficiency']['EER']

    out_comm = config.params['demand_commodities']['space cooling']

    for tech, row in config.nrcan_techs.iterrows():
        if row['end_use'] != 'space cooling': continue

        note = "(PJ/PJ) new build efficiency per vintage"

        # Get the NRCan nomenclature of the tech
        nrcan_stock = row['nrcan_stocks']

        # Input commodity
        in_comm = fuel_commodities.loc[row.loc['fuels'], 'comm']

        # Write single fuel efficiencies to database
        for vint in config.tech_vints[tech]:
            
            # Efficiency is new build efficiency for that year, or 2020 at the latest
            eff = t27_new_eff.loc[nrcan_stock, min(vint, max(np.array(t27_new_eff.columns, dtype=int)))]

            curs.execute(f"""REPLACE INTO
                Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES('{region}', '{in_comm}', '{tech}', {vint}, '{out_comm}', {eff}, '{note}',
                '{nrcan_ref}', {nrcan_year}, 1, 1, 1, 1, 1, 1)""")

    

    """
    ##############################################################
        Existing Capacity
    ##############################################################
    """

    # Existing cooling stock from NRCan data
    t27_stk = t27.loc[3:4].rename(columns={'Unnamed: 1':'tech'}).drop("Unnamed: 0", axis=1).set_index('tech').dropna()*1000 # units

    # Notes for database
    note = f"{nrcan_year} stock (NRCan, {nrcan_year}) indexed to population (Statcan, {statcan_year}) and distributed evenly over feasible past vintages."
    reference = f"{nrcan_ref}; {statcan_ref}"

    # Get existing capacities from NRCan stock and distribute over past vintages
    for tech, row in nrcan_techs.iterrows():
        if row['end_use'] != 'space cooling': continue

        nrcan_stock = row.loc['nrcan_stocks']

        # Get existing capacity (stock) from nrcan and index to population growth
        existing_cap = t27_stk.loc[nrcan_stock, nrcan_year] / 1E6 # Munit
        
        # Index to population and distribute existing capacities evenly over feasible vintages
        vints = config.tech_vints[tech]
        existing_cap *= pop.loc[config.model_periods[0]].values[0] / pop.loc[nrcan_year].values[0] / len(vints)

        # Write existing capacities to database
        for vint in vints:
            curs.execute(f"""REPLACE INTO
                        ExistingCapacity(regions, tech, vintage, exist_cap, exist_cap_units, exist_cap_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {vint}, {existing_cap}, 'Munit', '{note}',
                        '{reference}', {nrcan_year}, 1, 1, 1, {utils.dq_time(config.model_periods[0], nrcan_year)}, 1, 1)""")
        


    """
    ##############################################################
        Annual Capacity Factor
    ##############################################################
    """

    ## NRCan existing stock
    max_note = (f"Annual utilisation of units. (annual secondary energy consumption * efficiency) / (c2a * existing stock) (NRCan, {nrcan_year})")
    min_note = "99% of MaxACF. " + max_note

    # Get existing capacities from NRCan stock and distribute over past vintages
    for tech, row in nrcan_techs.iterrows():
        if row['end_use'] != 'space cooling': continue

        nrcan_stock = row.loc['nrcan_stocks']

        existing_cap = sum([fetch[0] for fetch in curs.execute(f"SELECT exist_cap FROM ExistingCapacity WHERE tech == '{tech}'").fetchall()])
        act = activity[nrcan_year].loc[nrcan_stock] # annual PJ output
        c2a = config.params['c2a']['space cooling']

        # Annual capacity factor is actual annual activity divided by max possible annual activity from arbitrary c2a
        acf = act / (existing_cap * c2a)

        for period in config.model_periods:
            curs.execute(f"""REPLACE INTO
                            MinAnnualCapacityFactor(regions, periods, tech, output_comm, min_acf, min_acf_notes,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{tech}', '{out_comm}', {acf*0.99}, '{min_note}',
                            '{reference}', {nrcan_year}, 1, 1, 1, {utils.dq_time(period, nrcan_year)}, 1, 1)""")
            curs.execute(f"""REPLACE INTO
                            MaxAnnualCapacityFactor(regions, periods, tech, output_comm, max_acf, max_acf_notes,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{tech}', '{out_comm}', {acf}, '{max_note}',
                            '{reference}', {nrcan_year}, 1, 1, 1, {utils.dq_time(period, nrcan_year)}, 1, 1)""")



    conn.commit()
    conn.close()