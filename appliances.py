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

# Shortens lines a bit
base_year = config.params['base_year']
nrcan_ref = config.params['nrcan_reference']
aeo_ref = config.params['aeo_reference']
aeo_year = config.params['aeo_data_year']
statcan_year = config.params['statcan_data_year']
statcan_ref = config.params['statcan_reference']
acf = config.params['appliances']['annual_capacity_factor']
end_use_demands = config.end_use_demands
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
        Annual Capacity Factor
    ##############################################################
    """

    ## NRCan existing stock
    max_note = "Arbitrary annual capacity factor to ensure that peak demand is met."
    min_note = "99% of MaxACF. " + max_note

    # Get existing capacities from NRCan stock and distribute over past vintages
    for tech, row in nrcan_techs.iterrows():
        if 'appliances' not in row['end_use']: continue

        out_comm = end_use_demands.loc[row['end_use'], 'comm']

        for period in config.model_periods:
            if row['end_use'] == 'appliances other': continue # no expansion so creates bugs
            if max(config.tech_vints[tech]) + config.lifetimes[tech] <= period: continue

            curs.execute(f"""REPLACE INTO
                            MinAnnualCapacityFactor(regions, periods, tech, output_comm, min_acf, min_acf_notes, dq_est)
                            VALUES('{region}', {period}, '{tech}', '{out_comm}', {acf*0.99}, '{min_note}', 0)""")
            curs.execute(f"""REPLACE INTO
                            MaxAnnualCapacityFactor(regions, periods, tech, output_comm, max_acf, max_acf_notes, dq_est)
                            VALUES('{region}', {period}, '{tech}', '{out_comm}', {acf}, '{max_note}', 0)""")



    """
    ##############################################################
        Existing Capacity
    ##############################################################
    """

    note = f"{base_year} stock (NRCan, {base_year}) distributed evenly over feasible preceding vintages."
    reference = nrcan_ref

    # Table 31: Appliance Stock by Appliance Type and Energy Source
    t31_elc_stk = utils.get_compr_db(region, 31, 20, 26)/1000 # Munit
    t31_ng_stk = utils.get_compr_db(region, 31, 38, 39)/1000 # Munit
    pop = config.populations[region]

    dems = dict() # sums up demand by end use
    for tech, row in nrcan_techs.iterrows():
        if 'appliances' not in row['end_use']: continue

        if row['fuels'] == 'electricity':
            exs_cap = t31_elc_stk.loc[row['nrcan_stocks'], base_year]
        elif row['fuels'] == 'natural gas':
            exs_cap = t31_ng_stk.loc[row['nrcan_stocks'], base_year]

        # Add to demand for this end use
        if row['end_use'] not in dems.keys(): dems[row['end_use']] = 0
        dems[row['end_use']] += exs_cap * end_use_demands.loc[row['end_use'], 'c2a'] * acf

        # Index to population and distribute existing capacities evenly over feasible vintages
        vints = [base_year] if row['end_use'] == 'appliances other' else config.tech_vints[tech]

        # Write existing capacities to database
        for vint in vints:
            if row['end_use'] != 'appliances other': # appliances other has no lifetime
                if vint + config.lifetimes[tech] <= config.model_periods[0]: continue

            curs.execute(f"""REPLACE INTO
                        ExistingCapacity(regions, tech, vintage, exist_cap, exist_cap_units, exist_cap_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {vint}, {exs_cap / len(vints)}, '(Munit)', '{note}',
                        '{reference}', {base_year}, 1, 1, 1, {utils.dq_time(config.model_periods[0], base_year)}, 1, 1)""")
        


    """
    ##############################################################
        Demand
    ##############################################################
    """

    note = (f"Existing capacity multiplied by an arbitrary {acf} annual capacity factor to ensure demand is met in peak hour. "
            f"Indexed to projected population (Statcan, {statcan_year}).")
    reference = f"{nrcan_ref}; {statcan_ref}"

    for end_use, exs_dem in dems.items():
        for period in config.model_periods:

            dem = exs_dem * pop.loc[period].values[0] / pop.loc[base_year].values[0]

            curs.execute(f"""REPLACE INTO
                    Demand(regions, periods, demand_comm, demand, demand_units, demand_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', {period}, '{end_use_demands.loc[end_use, 'comm']}', {dem}, '(Munity)', '{note}',
                    '{reference}', {base_year}, 1, 1, 1, {utils.dq_time(period, base_year)}, 1, 1)""")
        


    """
    ##############################################################
        Existing efficiency
    ##############################################################
    """

    # Table 13: Appliance Secondary Energy Use and GHG Emissions by Appliance Type
    t13_sec = utils.get_compr_db("ON", 13, 2, 9) # PJ

    ## Efficiency of electricity-only techs from NRCan
    for tech, row in nrcan_techs.iterrows():
        if 'appliances' not in row['end_use']: continue
        if row['end_use'] in ['appliances clothes dryers', 'appliances cooking ranges']: continue

        vints = [base_year] if row['end_use'] == 'appliances other' else config.tech_vints[tech]

        note = (f"(Munity/PJ) {base_year} demand divided by {base_year} secondary energy consumption (NRCan, {base_year}). ")

        stock = t31_elc_stk.loc[row['nrcan_stocks'], base_year]
        sec = t13_sec.loc[row['nrcan_stocks'], base_year]

        # Times acf because assumed actual activity is stock times acf
        eff_exs = stock * acf / sec

        ## Existing Efficiency
        for vint in vints:
            if row['end_use'] != 'appliances other': # appliances other has no lifetime
                if vint + config.lifetimes[tech] <= config.model_periods[0]: continue

            curs.execute(f"""REPLACE INTO
                    Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', '{fuel_commodities.loc['electricity', 'comm']}', '{tech}', {vint}, '{end_use_demands.loc[row['end_use'], 'comm']}', {eff_exs}, '{note}',
                    '{nrcan_ref}', {base_year}, 1, 1, 1, 1, 1, 1)""")


    ## Cooking ranges and clothes dryers
    # A pain to deal with because both natural gas and electricity variants
    note = "(Munity/PJ) From generic unit energy consumpion (UEC) of existing stock from Energy Use Data Handbook as provincial data cannot be disaggregated by both end use and fuel."
    reference = config.params['handbook_reference']

    # Generic unit energy consumption of nrcan technologies
    hb_uec = utils.get_data(f"https://oee.nrcan.gc.ca/corporate/statistics/neud/dpa/data_e/downloads/handbook/Excel/{2020}/res_00_16_e.xls", skiprows=7)
    hb_uec = hb_uec.drop('Unnamed: 0', axis=1).set_index('Unnamed: 1').dropna() * config.params['conversion_factors']['activity']['kwh'] * 1E6 # kWh/unity to PJ/Munity
    utils.clean_index(hb_uec)
    hb_uec_elc = hb_uec.iloc[8:14]
    hb_uec_ng = hb_uec.iloc[14:16]

    fuels = ['electricity', 'natural gas'] # fuels to deal with
    hb_uecs = [hb_uec_elc, hb_uec_ng] # reciprocal of base efficiency is "energy consumption"
    
    # Calculate efficiencies for each fuel in Munity/PJ
    for end_use in ['appliances clothes dryers', 'appliances cooking ranges']:

        # Both ng and elc technologies for this end use
        techs = [nrcan_techs.loc[(nrcan_techs['end_use']==end_use) & (nrcan_techs['fuels']==fuel)].index.values[0] for fuel in fuels]

        for f in [0,1]:

            row = nrcan_techs.loc[techs[f]] # configuration data
            vints = config.tech_vints[techs[f]]

            # Efficiency in Munity/PJ times acf because assumed actual activity is stock times acf
            eff_exs = 1/hb_uecs[f].loc[row['nrcan_stocks'], base_year] * acf

            ## Existing Efficiency
            for vint in vints:
                if vint + config.lifetimes[techs[f]] <= config.model_periods[0]: continue
                
                curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{fuel_commodities.loc[fuels[f], 'comm']}', '{techs[f]}', {vint}, '{end_use_demands.loc[end_use, 'comm']}', {eff_exs}, '{note}',
                        '{reference}', {base_year}, 3, 2, 1, 1, 3, 1)""")
            


    """
    ##############################################################
        New stock efficiency
    ##############################################################
    """

    reference = f"{nrcan_ref}; {aeo_ref}"

    # AEO data relevant to this region
    df0 = aeo_res_equip.loc[(aeo_res_equip['Census Division'] == config.regions.loc[region, 'us_census_div']) | (aeo_res_equip['Census Division'] == 11)]

    for tech, row in aeo_techs.iterrows():
        if 'appliances' not in row['end_uses']: continue

        # Relevant to this tech
        df1 = df0.loc[row['aeo_equip']]
        
        # Get baseline efficiency from existing stock
        nrcan_tech = nrcan_techs.loc[nrcan_techs['end_use'] + " - " + nrcan_techs['description'] == row['nrcan_equiv']].index.values[0]
        base_eff = aeo_res_class.loc[row['aeo_class'], 'Base Efficiency']
        eff_exs = curs.execute(f"SELECT efficiency FROM Efficiency WHERE regions == '{region}' and tech == '{nrcan_tech}'").fetchone()[0]

        note = (f"(Munity/PJ) Efficiency assumed same as {nrcan_tech} "
                f"but indexed to relative efficiency for this vintage versus baseline efficiency from AEO data (AEO, {aeo_year}).")

        vints = config.tech_vints[tech]
        for vint in vints:

            # Relevant to this vintage
            if type(df1) is pd.DataFrame: new_eff = df1.loc[(df1['First Year']<=vint) & (vint<=df1['Last Year']), 'Efficiency'].values[0]
            elif type(df1) is pd.Series: new_eff = df1['Efficiency'] # only one row remaining

            if new_eff >= base_eff: eff = eff_exs * new_eff / base_eff
            else: eff = eff_exs * base_eff / new_eff # efficiency units are energy consumption so invert

            # Write to table
            curs.execute(f"""REPLACE INTO
                    Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', '{fuel_commodities.loc[row['fuel'], 'comm']}', '{tech}', {vint}, '{end_use_demands.loc[row['end_uses'], 'comm']}', {eff}, '{note}',
                    '{aeo_ref}', {aeo_year}, 3, 2, 1, 1, 3, 3)""")

    conn.commit()
    conn.close()