"""
Aggregates data for residential space cooling
Written by Ian David Elder for the CANOE model
"""

import utils
import pandas as pd
import os
import numpy as np
import sqlite3
from setup import config

# Shortens lines a bit
base_year = config.params['base_year']
nrcan_ref = config.params['nrcan_reference']
statcan_year = config.params['statcan_data_year']
statcan_ref = config.params['statcan_reference']
fuel_commodities = config.fuel_commodities
nrcan_techs = config.nrcan_techs
space_cooling = config.end_use_demands.loc['space cooling']


def aggregate():

    for region in config.model_regions: aggregate_region(region)
    
    print(f"Space cooling data aggregated into {os.path.basename(config.database_file)}\n")



def aggregate_region(region):

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    """
    ##############################################################
        Demand
    ##############################################################
    """

    note = (f"Sum of {base_year} secondary energy multiplied by efficiency per technology (NRCan, {base_year}). "
            f"Indexed to projected population (Statcan, {statcan_year})")
    reference = f"{nrcan_ref}; {statcan_ref}"

    # Table 4: Space Cooling Secondary Energy Use and GHG Emissions by Cooling System Type
    t4_sec = utils.get_compr_db(region, 4, 3, 4)

    # Table 27: Cooling System Stock by Type, New Unit Efficiencies, Stock Efficiencies and Unit Capacity Ratio
    t27_stk_eff = utils.get_compr_db(region, 27, 15, 16)
    t27_stk_eff.index=t4_sec.index
    t27_stk_eff *= config.params['conversion_factors']['efficiency']['EER']

    # Activity (PJ output) is secondary energy times efficiency, and demand is sum of activity
    activity = t4_sec.values * t27_stk_eff.values
    activity = pd.DataFrame(data=activity, columns=t4_sec.columns, index=t4_sec.index)

    # Index demand to population growth
    pop = config.populations[region]
    dem = activity[base_year].sum() * pop / pop.loc[base_year]

    # Write to database
    for period in config.model_periods:
        curs.execute(f"""REPLACE INTO
                    Demand(regions, periods, demand_comm, demand, demand_units, demand_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', {period}, '{space_cooling['comm']}', {float(dem.loc[period])}, '({space_cooling['dem_unit']})', '{note}',
                    '{reference}', {base_year}, 1, 1, 1, {utils.dq_time(period, base_year)}, 1, 1)""")



    """
    ##############################################################
        Efficiency of existing stock
    ##############################################################
    """

    for tech, row in config.nrcan_techs.iterrows():
        if row['end_use'] != 'space cooling': continue

        # Input commodity
        in_comm = fuel_commodities.loc[row.loc['fuels']]

        note = f"({space_cooling['dem_unit']}/{in_comm['unit']}) new build efficiency per vintage"

        # Get the NRCan nomenclature of the tech
        nrcan_stock = row['nrcan_stocks']

        # Write single fuel efficiencies to database
        for vint in config.tech_vints[tech]:
            if vint + config.lifetimes[tech] <= config.model_periods[0]: continue
            
            # Efficiency is new build efficiency for that year, or 2020 at the latest
            eff = t27_stk_eff.loc[nrcan_stock, min(vint, max(np.array(t27_stk_eff.columns, dtype=int)))]

            curs.execute(f"""REPLACE INTO
                Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES('{region}', '{in_comm['comm']}', '{tech}', {vint}, '{space_cooling['comm']}', {eff}, '{note}',
                '{nrcan_ref}', {base_year}, 1, 1, 1, 1, 1, 1)""")

    

    """
    ##############################################################
        Existing Capacity and Annual Capacity Factor
    ##############################################################
    """

    # Existing cooling stock from NRCan data
    t27_stk = utils.get_compr_db(region, 27, 3, 4)/1000 # Munit

    # Notes for database
    note = f"{base_year} stock (NRCan, {base_year}) distributed evenly over feasible preceding vintages."
    reference = nrcan_ref

    # Get existing capacities from NRCan stock and distribute over past vintages
    for tech, row in nrcan_techs.iterrows():
        if row['end_use'] != 'space cooling': continue

        nrcan_stock = row.loc['nrcan_stocks']


        ## Existing capacity
        # Get existing capacity (stock) from nrcan and index to population growth
        existing_cap = t27_stk.loc[nrcan_stock, base_year]
        
        # Index to population and distribute existing capacities evenly over feasible vintages
        vints = config.tech_vints[tech]

        # Write existing capacities to database
        for vint in vints:
            if vint + config.lifetimes[tech] <= config.model_periods[0]: continue

            curs.execute(f"""REPLACE INTO
                        ExistingCapacity(regions, tech, vintage, exist_cap, exist_cap_units, exist_cap_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {vint}, {existing_cap / len(vints)}, '({space_cooling['cap_unit']})', '{note}',
                        '{reference}', {base_year}, 1, 1, 1, {utils.dq_time(config.model_periods[0], base_year)}, 1, 1)""")


        ## Annual capacity factor for NRCan existing stock
        # (for new stock pulled in all sectors post processing)
        max_note = (f"Annual utilisation of units. (annual secondary energy consumption * efficiency) / (c2a * existing stock) (NRCan, {base_year})")
        min_note = "99% of MaxACF. " + max_note

        act = activity[base_year].loc[nrcan_stock] # annual PJ output
        c2a = config.end_use_demands.loc['space cooling', 'c2a']

        # Annual capacity factor is actual annual activity divided by max possible annual activity from arbitrary c2a
        acf = act / (existing_cap * c2a)

        for period in config.model_periods:
            if max(vints) + config.lifetimes[tech] <= period: continue
            
            curs.execute(f"""REPLACE INTO
                            MinAnnualCapacityFactor(regions, periods, tech, output_comm, min_acf, min_acf_notes,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{tech}', '{space_cooling['comm']}', {acf*0.99}, '{min_note}',
                            '{reference}', {base_year}, 1, 1, 1, {utils.dq_time(period, base_year)}, 1, 1)""")
            curs.execute(f"""REPLACE INTO
                            MaxAnnualCapacityFactor(regions, periods, tech, output_comm, max_acf, max_acf_notes,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{tech}', '{space_cooling['comm']}', {acf}, '{max_note}',
                            '{reference}', {base_year}, 1, 1, 1, {utils.dq_time(period, base_year)}, 1, 1)""")



    conn.commit()
    conn.close()



if __name__ == "__main__":
    
    aggregate()