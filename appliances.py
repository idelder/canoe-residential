"""
Aggregates data for appliances
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
            "Dual fuel boilers taken to consume only first listed fuel in this calculation. "
            f"Indexed to projected population (Statcan, {statcan_year})")
    reference = f"{nrcan_ref}; {statcan_ref}"

    # Table 8: Space Heating Secondary Energy Use by System Type
    t8 = utils.get_data(utils.compr_db_url(region, 8), skiprows=10)
    t8_sec = t8.loc[3:17].rename(columns={'Unnamed: 1':'tech'}).drop("Unnamed: 0", axis=1).set_index('tech').dropna()
    utils.clean_index(t8_sec)

    # Table 26: Heating System Stock Efficiencies
    t26 = utils.get_data(utils.compr_db_url(region, 26), skiprows=10)
    t26_eff = t26.loc[2:27].rename(columns={'Unnamed: 1':'tech'}).drop("Unnamed: 0", axis=1).set_index('tech').dropna()/100
    utils.clean_index(t26_eff)

    # Multiply secondary energy by efficiency to get output heating energy
    # Dual fuel systems make this a little painful
    activity = t8_sec.copy()[nrcan_year] # output primary energy (demand) by technology
    tracker = dict() # tracking multiples of the same fuel type
    for nrcan_tech in activity.index:

        fuel = nrcan_tech
        if "/" in fuel: fuel = fuel.split("/")[0]

        eff = t26_eff.loc[fuel, nrcan_year]

        # To handle multple techs of the same fuel, take efficiencies from the table in order
        if fuel not in tracker.keys(): tracker[fuel] = 0
        if type(eff) is pd.Series:
            eff = eff[tracker[fuel]]
            tracker[fuel] += 1

        # Demanded primary energy is secondary input energy times efficiency
        # Dual fuel systems are assumed to consume the first listed fuel for simplicity
        activity.loc[nrcan_tech] *= eff

    # Index demand to population growth
    pop = config.populations[region]
    dem = activity.sum() * pop / pop.loc[nrcan_year]

    # Write to database
    for period in config.model_periods:
        curs.execute(f"""REPLACE INTO
                    Demand(regions, periods, demand_comm, demand, demand_units, demand_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', {period}, '{config.params['demand_commodities']['space heating']}', {float(dem.loc[period])}, 'PJ', '{note}',
                    '{reference}', {nrcan_year}, 1, 1, 1, {utils.dq_time(period, nrcan_year)}, 1, 1)""")



    """
    ##############################################################
        Efficiency of existing stock
    ##############################################################
    """

    out_comm = config.params['demand_commodities']['space heating']

    tracker = dict() # tracking multiples of the same nrcan fuel type
    for tech, row in config.nrcan_techs.iterrows():
        if row['end_use'] != 'space heating': continue

        note = "(PJ/PJ)"

        # Get the NRCan nomenclature of the tech
        nrcan_stock = row['nrcan_stocks']
        if pd.isna(nrcan_stock): continue # skip empty rows

        ## Dual fuel boiler, needs two efficiencies
        # These are uncommon so making a lot of simplifications and only including existing
        if "/" in nrcan_stock:
            nrcan_fuels = nrcan_stock.split("/")
            fuels = row.loc['fuels'].split("+")

            # Add an efficiency for each fuel
            for f in [0,1]:
                fuel = nrcan_fuels[f]

                fuel = fuel.replace('Electric','Electricity') # annoying
                eff = t26_eff.loc[fuel, nrcan_year]

                # To handle multple techs of the same fuel, take efficiencies from the table in order, (skipping one for wood, annoying)
                if fuel not in tracker.keys(): tracker[fuel] = 1 if fuel == 'Wood' else 0
                if type(eff) is pd.Series: # multiples exist
                    eff = eff[tracker[fuel]] # take the next in the list
                    tracker[fuel] += 1 # move up the tracker

                # Input commodity
                in_comm = fuel_commodities.loc[fuels[f], 'comm']

                # Write dual fuel efficiencies to database
                for vint in config.tech_vints[tech]:
                    curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{in_comm}', '{tech}', {vint}, '{out_comm}', {eff}, '{note}',
                        '{nrcan_ref}', {nrcan_year}, 1, 1, 1, 1, 1, 1)""")
    
            continue


        # Input commodity for others
        in_comm = fuel_commodities.loc[row.loc['fuels'], 'comm']

        ## Technologies aggregated from multiple NRCan stocks
        if "+" in nrcan_stock:

            substocks = nrcan_stock.split("+")
            note = f"(PJ/PJ) aggregated efficiencies proportioned to secondary energy consumption"

            sec_energy = np.array([t8_sec.loc[substock, nrcan_year] for substock in substocks])
            effs = np.array([t26_eff.loc[substock, nrcan_year] for substock in substocks])

            # Efficiency is aggregated by ratios of secondary energy consumption in data year
            # This matches the demand calculation which is efficiency multiplied by secondary energy consumption
            eff = np.dot(effs, sec_energy) / sum(sec_energy)

        ## Single fuel and single stock technologies    
        else:
            eff = t26_eff.loc[nrcan_stock, nrcan_year]
            if type(eff) is pd.Series: eff = eff.iloc[0] # these technologies are above dual fuel boilers in the table


        # Write single fuel efficiencies to database
        for vint in config.tech_vints[tech]:
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

    # Table 21: Heating System Stock by Building Type and Heating System Type
    t21 = utils.get_data(utils.compr_db_url(region, 21), skiprows=10)
    t21_stk = t21.loc[16:30].rename(columns={'Unnamed: 1':'tech'}).drop("Unnamed: 0", axis=1).set_index('tech').dropna()*1000 # units
    utils.clean_index(t21_stk)

    # Notes for database
    note = f"{nrcan_year} stock (NRCan, {nrcan_year}) indexed to population (Statcan, {statcan_year}) and distributed evenly over feasible past vintages."
    reference = f"{nrcan_ref}; {statcan_ref}"

    # Get existing capacities from NRCan stock and distribute over past vintages
    for tech, row in nrcan_techs.iterrows():
        if row['end_use'] != 'space heating': continue

        nrcan_stocks = row.loc['nrcan_stocks']
        if pd.isna(nrcan_stocks): continue # no existing stock from NRCan

        substocks = nrcan_stocks.split("+")

        # Get existing capacity (stock) from nrcan and index to population growth
        existing_cap = sum([t21_stk.loc[substock, nrcan_year] for substock in substocks]) / 1E6 # Munit
        
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
        if row['end_use'] != 'space heating': continue

        nrcan_stocks = row.loc['nrcan_stocks']
        if pd.isna(nrcan_stocks): continue # no existing stock from NRCan

        substocks = nrcan_stocks.split("+")

        existing_cap = sum([fetch[0] for fetch in curs.execute(f"SELECT exist_cap FROM ExistingCapacity WHERE tech == '{tech}'").fetchall()])
        act = sum([activity.loc[substock] for substock in substocks]) # annual PJ output
        c2a = config.params['c2a']['space heating']

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