"""
Aggregates data for residential space heating
Written by Ian David Elder for the CANOE model
"""

import utils
import pandas as pd
import os
from scipy.special import gamma
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


def aggregate(region):

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    """
    ##############################################################
        Demand
    ##############################################################
    """

    note = (f"Sum of {base_year} secondary energy multiplied by efficiency per technology (NRCan, {base_year}). "
            "Dual fuel boilers taken to consume only first listed fuel in this calculation. "
            f"Indexed to projected population (Statcan, {statcan_year})")
    reference = f"{nrcan_ref}; {statcan_ref}"

    # Table 8: Space Heating Secondary Energy Use by System Type
    t8_sec = utils.get_compr_db(region, 8, 3, 17)

    # Table 26: Heating System Stock Efficiencies
    t26_eff = utils.get_compr_db(region, 26, 2, 27)/100

    # Multiply secondary energy by efficiency to get output heating energy
    # Dual fuel systems make this a little painful
    activity = t8_sec.copy()[base_year] # output primary energy (demand) by technology
    tracker = dict() # tracking multiples of the same fuel type
    for nrcan_tech in activity.index:

        fuel = nrcan_tech
        if "/" in fuel: fuel = fuel.split("/")[0]

        eff = t26_eff.loc[fuel, base_year]

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
    dem = activity.sum() * pop / pop.loc[base_year]

    out_comm = config.end_use_demands.loc['space heating', 'comm']

    # Write to database
    for period in config.model_periods:
        curs.execute(f"""REPLACE INTO
                    Demand(regions, periods, demand_comm, demand, demand_units, demand_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', {period}, '{out_comm}', {float(dem.loc[period])}, '(PJ)', '{note}',
                    '{reference}', {base_year}, 1, 1, 1, {utils.dq_time(period, base_year)}, 1, 1)""")



    """
    ##############################################################
        Efficiency of existing stock
    ##############################################################
    """

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
                eff = t26_eff.loc[fuel, base_year]

                # To handle multple techs of the same fuel, take efficiencies from the table in order, (skipping one for wood, annoying)
                if fuel not in tracker.keys(): tracker[fuel] = 1 if fuel == 'Wood' else 0
                if type(eff) is pd.Series: # multiples exist
                    eff = eff[tracker[fuel]] # take the next in the list
                    tracker[fuel] += 1 # move up the tracker

                # Input commodity
                in_comm = fuel_commodities.loc[fuels[f], 'comm']

                # Write dual fuel efficiencies to database
                for vint in config.tech_vints[tech]:
                    if vint + config.lifetimes[tech] <= config.model_periods[0]: continue

                    curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{in_comm}', '{tech}', {vint}, '{out_comm}', {eff}, '{note}',
                        '{nrcan_ref}', {base_year}, 1, 1, 1, 1, 1, 1)""")
    
            continue


        # Input commodity for others
        in_comm = fuel_commodities.loc[row.loc['fuels'], 'comm']

        ## Technologies aggregated from multiple NRCan stocks
        if "+" in nrcan_stock:

            substocks = nrcan_stock.split("+")
            note = f"(PJ/PJ) aggregated efficiencies proportioned to secondary energy consumption"

            sec_energy = np.array([t8_sec.loc[substock, base_year] for substock in substocks])
            effs = np.array([t26_eff.loc[substock, base_year] for substock in substocks])

            # Efficiency is aggregated by ratios of secondary energy consumption in data year
            # This matches the demand calculation which is efficiency multiplied by secondary energy consumption
            eff = np.dot(effs, sec_energy) / sum(sec_energy)

        ## Single fuel and single stock technologies    
        else:
            eff = t26_eff.loc[nrcan_stock, base_year]
            if type(eff) is pd.Series: eff = eff.iloc[0] # these technologies are above dual fuel boilers in the table


        # Write single fuel efficiencies to database
        for vint in config.tech_vints[tech]:
            if vint + config.lifetimes[tech] <= config.model_periods[0]: continue
            
            curs.execute(f"""REPLACE INTO
                Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                VALUES('{region}', '{in_comm}', '{tech}', {vint}, '{out_comm}', {eff}, '{note}',
                '{nrcan_ref}', {base_year}, 1, 1, 1, 1, 1, 1)""")
    


    """
    ##############################################################
        Existing Capacity and Annual Capacity Factor
    ##############################################################
    """

    # Table 21: Heating System Stock by Building Type and Heating System Type
    t21_stk = utils.get_compr_db(region, 21, 16, 30)/1000 # Munit

    # Notes for database
    note = f"{base_year} stock (NRCan, {base_year}) distributed evenly over feasible preceding vintages."
    reference = nrcan_ref

    # Get existing capacities from NRCan stock and distribute over past vintages
    for tech, row in nrcan_techs.iterrows():
        if row['end_use'] != 'space heating': continue

        nrcan_stocks = row.loc['nrcan_stocks']
        if pd.isna(nrcan_stocks): continue # no existing stock from NRCan

        substocks = nrcan_stocks.split("+")


        ## Existing capacity
        # Get existing capacity (stock) from nrcan and index to population growth
        existing_cap = sum([t21_stk.loc[substock, base_year] for substock in substocks])
        
        # Distribute existing capacities evenly over feasible vintages
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

        act = sum([activity.loc[substock] for substock in substocks]) # annual PJ output
        c2a = config.end_use_demands.loc['space heating', 'c2a']

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



    """
    ##############################################################
        Furnace fan electricity consumption
    ##############################################################
    """

    frn_fan = config.params['furnace_fans'] # parameters relating to furnace fans
    eff = frn_fan['efficiency']
    split = eff * frn_fan['output_split'] / (1 + eff * frn_fan['output_split']) # calculating TOS to achieve correct electricity consumption
    tos_note = f"(PJ/PJ) Furnace fan electricity consumption. x/(1+x) where x is assumed 6 kWh into fan / MMBtu out (nyserda, 2013)."

    # Get technologies that need furnace fan consumption
    fan_classes = config.aeo_res_class.loc[config.aeo_res_class['Furnace Fan Flag']==1].index.unique()
    fan_techs = [*config.nrcan_techs.loc[config.nrcan_techs['aeo_class'].isin(fan_classes)].index.values, *config.aeo_techs.loc[config.aeo_techs['aeo_class'].isin(fan_classes)].index.values]

    for tech in fan_techs:

        vints = config.tech_vints[tech]

        # Add a dummy process to convert input fan electricity to worthless dummy commodity
        # 100% efficiency so TechOutputSplit can be used
        for vint in vints:
            if vint + config.lifetimes[tech] <= config.model_periods[0]: continue

            curs.execute(f"""REPLACE INTO
                    Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes, dq_est)
                    VALUES('{region}', '{config.fuel_commodities.loc['electricity', 'comm']}', '{tech}', {vint},
                    '{frn_fan['output_commodity']}', {eff}, '(PJ/PJ) arbitrarily small non-zero efficiency', 0)""")
        
        # Set ratio of fan electricity consumption to output heat
        for period in config.model_periods:
            if max(vints) + config.lifetimes[tech] <= period: continue

            curs.execute(f"""REPLACE INTO
                    TechOutputSplit(regions, periods, tech, output_comm, to_split, to_split_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', {period}, '{tech}', '{frn_fan['output_commodity']}', {split}, '{tos_note}',
                    '{frn_fan['reference']}', {2013}, 3, 1, 1, {utils.dq_time(2013, period)}, 3, 3)""")
    

    conn.commit()
    conn.close()