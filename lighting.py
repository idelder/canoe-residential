"""
Aggregates data for residential lighting
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
statcan_year = config.params['statcan_data_year']
statcan_ref = config.params['statcan_reference']
nrcan_year = config.params['nrcan_data_year']
nrcan_ref = config.params['nrcan_reference']
aeo_ref = config.params['aeo_reference']
aeo_year = config.params['aeo_data_year']
on_stock_ref = config.params['lighting']['on_stock_ref']
usage_ref = config.params['lighting']['usage_ref']
curr = config.params['aeo_currency']
curr_year = config.params['aeo_currency_year']
conv = config.params['conversion_factors']['lighting']

# Some common variables
in_comm = config.fuel_commodities.loc[config.params['lighting']['input_comm'], 'comm']
out_comm = config.params['demand_commodities']['lighting']
acf = config.params['lighting']['annual_capacity_factor']



"""
##############################################################
    Non-regional setup
##############################################################
"""

# Get provincial data on relative usage of different bulb types from Statcan table 38100048
lgt_usage = utils.get_statcan_table(38100048)
lgt_usage['GEO'] = lgt_usage['GEO'].str.lower()

# Configuration file for lighting technologies, including Ontario shares data from residential end use survey
exs_techs = pd.read_csv(input_files + '/existing_lighting.csv', index_col=0)
aeo_techs = pd.read_csv(input_files + '/aeo_lighting_data.csv', index_col=0)

# Gets a value from aeo lighting data
def get_aeo_value(tech, metric, vintage):
    
    # Get the latest preceding vintage
    vints = np.array([int(col) for col in aeo_techs.columns if col.isdecimal()])
    last_vint = vints[vints < vintage][-1]

    # If no value for that vintage, take existing stock value
    value = aeo_techs.loc[aeo_techs['metric']==metric].loc[tech, str(last_vint)]
    if pd.isna(value): value = aeo_techs.loc[aeo_techs['metric']==metric].loc[tech, 'existing']
    
    return value

# Gets relative usage rates of bulb types for a province from Statcan table 38100048
def get_usage(region):

    # Just filtering and pivoting the table to show bulb types as rows and years as columns
    usage = lgt_usage.loc[(lgt_usage['GEO'] == config.regions.loc[region, 'description'])][['Type of energy-saving light','REF_DATE','VALUE']].set_index('Type of energy-saving light')
    usage = usage.pivot_table(values='VALUE', index=usage.index, columns='REF_DATE', aggfunc='first')

    # The residential end use survey was 2018 so interpolate between 2017/2019
    return (usage[2017] + usage[2019])/2

# Ontario usage as a baseline
on_usage = get_usage('ON')



def aggregate(region):

    # Connect to the new database file
    conn = sqlite3.connect(database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    """
    ##############################################################
        Annual Capacity Factor
    ##############################################################
    """

    acf_note = config.params['lighting']['acf_note']
    min_note = acf_note + " 99% of max acf."
    acf_ref = config.params['lighting']['acf_reference']
    data_year = config.params['lighting']['acf_data_year']

    for tech in [*aeo_techs.index,*exs_techs.index]:
        for period in config.model_periods:

            dq_time = utils.dq_time(period, data_year)

            curs.execute(f"""REPLACE INTO
                            MinAnnualCapacityFactor(regions, periods, tech, output_comm, min_acf, min_acf_notes,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{tech}', '{out_comm}', {acf*0.99}, '{min_note}',
                            '{acf_ref}', {data_year}, 1, 1, 1, {dq_time}, 1, 1)""")
            curs.execute(f"""REPLACE INTO
                            MaxAnnualCapacityFactor(regions, periods, tech, output_comm, max_acf, max_acf_notes,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{tech}', '{out_comm}', {acf}, '{acf_note}',
                            '{acf_ref}', {data_year}, 1, 1, 1, {dq_time}, 1, 1)""")



    """
    ##############################################################
        Demand
    ##############################################################
    """

    note = (f"{nrcan_year} secondary energy (NRCan, {nrcan_year}) multiplied by average efficacy (efficiency) of existing lighting stock. "
            f"Indexed to projected population (Statcan, {statcan_year})")
    reference = f"{nrcan_ref}; {statcan_ref}"
    
    # Get usage of bulb types for this region relative to Ontario
    # Because we have actual shares data for Ontario (residential end use survey)
    reg_usage = get_usage(region)
    usage_index = reg_usage / on_usage

    # Calculate regional shares by indexing ontario shares to Statcan usage survey
    reg_shares = exs_techs.rename({'on_share_sf':'share_sf', 'on_share_mf':'share_mf'}, axis=1)
    for tech, shares in reg_shares.iterrows():
        statcan_cat = exs_techs.loc[tech, 'statcan_category']
        shares[['share_sf','share_mf']] *= usage_index.loc[statcan_cat]
    for col in reg_shares[['share_sf','share_mf']].columns: reg_shares[col] /= reg_shares[col].sum() # reset to sum 100%

    # Table 14: Total Households by Building Type and Energy Source
    t14 = utils.get_data(utils.compr_db_url(region, 14), skiprows=10)
    t14 = t14.loc[9:12].rename(columns={'Unnamed: 1':'housing_type'}).drop("Unnamed: 0", axis=1).set_index('housing_type').dropna()[2018] / 100 # % shares
    utils.clean_index(t14)
    
    # Aggregate subcategories of housing into single-family and multi-family
    for cat, subcats in config.params['housing_categories'].items():
        subcats = subcats.split('+')
        t14[cat] = sum([t14[subcat] for subcat in subcats])
        t14 = t14.drop(subcats)
    
    # Mapping existing stock AEO data to existing technologies
    aeo_equivs = dict()
    for tech, row in exs_techs.iterrows():
        aeo_data = aeo_techs.loc[(aeo_techs['description'].str.contains(row['description'])) & (~pd.isna(aeo_techs['existing']))]\
                            .pivot_table(values='existing', index='tech', columns='metric')
        aeo_equivs[tech] = aeo_data.index.values[0]
        for metric in aeo_data.columns: exs_techs.loc[tech, metric] = aeo_data[metric].values[0]
    
    # Unit conversion of efficacy lm/W to Glmy/PJ
    exs_techs['efficacy'] *= conv['efficacy']
    exs_techs['cost_maintain'] *= conv['cost']
    exs_techs['lamp_life'] = round(exs_techs['lamp_life'] * conv['lifetime'] / acf)

    # Finally, calculate the average efficacy of existing lighting stock, indexed to shares of single-family vs multi-family housing
    exs_eff = 0 # lm/W
    for tech_exs, row_exs in reg_shares.iterrows():
        reg_shares.loc[tech_exs, 'share_tot'] = np.dot(row_exs[['share_sf','share_mf']].values, t14.values)
        exs_eff += exs_techs.loc[tech_exs, 'efficacy'] * reg_shares.loc[tech_exs, 'share_tot']

    # Table 3: Lighting Secondary Energy Use and GHG Emissions
    t3 = utils.get_data(utils.compr_db_url(region, 3), skiprows=10)
    sec = t3.loc[1][nrcan_year]

    # Demand is secondary energy times 2018 average lighting stock efficacy, indexed to population growth
    pop = config.populations[region]
    dem = exs_eff * sec * pop / pop.loc[nrcan_year]

    # Write demand to database
    for period in config.model_periods:
        curs.execute(f"""REPLACE INTO
                    Demand(regions, periods, demand_comm, demand, demand_units, demand_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', {period}, '{config.params['demand_commodities']['lighting']}', {float(dem.loc[period])}, 'Glmy', '{note}',
                    '{reference}', {nrcan_year}, 3, 3, 1, {utils.dq_time(period, nrcan_year)}, 3, 1)""")
        


    """
    ##############################################################
        Existing stock data
    ##############################################################
    """

    # Existing capacity in Glmy at time of first model periods as indexed by population growth
    exs_techs['existing_capacity'] = reg_shares['share_tot'] * float(dem.loc[config.model_periods[0]] / acf)
    
    # Distribute existing capacities over feasible past vintages
    for tech, row in exs_techs.iterrows():

        lifetime = row['lamp_life']

        aeo_note = f"Assumed same as {aeo_equivs[tech]}."
        curs.execute(f"""REPLACE INTO
                    LifetimeTech(regions, tech, life, life_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', '{tech}', {lifetime}, '{aeo_note}',
                    '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")
        
        vints = utils.feasible_vintages(config.model_periods[0], lifetime)

        # LEDs didn't come around that long ago sp cap the oldest vintage
        if not pd.isna(row['oldest_vint']): vints = [vint for vint in vints if vint >= row['oldest_vint']]
        
        # Divide between feasible vintages
        exs_cap = row['existing_capacity'] / len(vints)

        # Write existing data to database
        for vint in vints:

            note = (f"Ontario existing stock of residential bulb types by housing type (IESO, 2018) "
                    f"multiplied by housing stock by type (NRCan, {nrcan_year}). "
                    f"Indexed to relative usage of bulb types by province versus Ontario (Statcan, 2018) "
                    f"and to projected population (Statcan, {statcan_year}).")
            reference = f"{on_stock_ref}; {nrcan_ref}; {usage_ref}; {statcan_ref}"
            curs.execute(f"""REPLACE INTO
                        ExistingCapacity(regions, tech, vintage, exist_cap, exist_cap_units, exist_cap_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {vint}, {exs_cap}, 'Glm', '{note}',
                        '{reference}', {2018}, 4, 2, 1, {utils.dq_time(config.model_periods[0], 2018)}, 3, 1)""")
            
            curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{in_comm}', '{tech}', {vint}, '{out_comm}', {row['efficacy']}, '{aeo_note}',
                        '{aeo_ref}', {2018}, 1, 1, 1, 1, 3, 1)""")
            
            for period in config.model_periods:

                curs.execute(f"""REPLACE INTO
                            CostFixed(regions, periods, tech, vintage, cost_fixed_notes, data_cost_fixed, data_cost_year, data_curr,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{tech}', {vint}, '{aeo_note}', {row['cost_maintain']}, {curr_year}, '{curr}',
                            '{aeo_ref}', {aeo_year}, 2, 1, 1, 1, 3, 3)""")
    


    """
    ##############################################################
        New stock data
    ##############################################################
    """

    for tech in set(aeo_techs.index):

        # Vintages for new stock are model periods
        for vint in config.model_periods:

            # Lifetime from aeo data converted from hours to years using the annual capacity factor and rounded
            lifetime =  round(get_aeo_value(tech, 'lamp_life', vint) * conv['lifetime'] / acf)

            ## LifetimeProcess
            # Using lifetime process because some bulb lives improve over model periods in aeo data
            note = f"Lamp life in hours (AEO, {aeo_year}) divided by annual capacity factor (DOE, 2012)."
            reference = f"{aeo_ref}; {acf_ref}"
            curs.execute(f"""REPLACE INTO
                        LifetimeProcess(regions, tech, vintage, life_process, life_process_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {vint}, {lifetime}, 'TODO',
                        '{reference}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")
            
            ## Efficiency
            efficacy = conv['efficacy'] * get_aeo_value(tech, 'efficacy', vint)
            curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{in_comm}', '{tech}', {vint}, '{out_comm}', {efficacy}, 'TODO',
                        'TODO', {2018}, 1, 1, 1, 1, 3, 1)""")
            
            ## CostInvest
            cost_invest = conv['cost'] * get_aeo_value(tech, 'cost_install', vint)
            curs.execute(f"""REPLACE INTO
                        CostInvest(regions, tech, vintage, data_cost_invest, data_cost_year, data_curr,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {vint}, {cost_invest}, {curr_year}, '{curr}',
                        '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")
            
            for period in config.model_periods:
                
                # Can't pay for a technology if it can't exist
                if period < vint or vint + lifetime <= period: continue
                
                ## CostFixed
                cost_fixed = conv['cost'] * get_aeo_value(tech, 'cost_maintain', vint)
                curs.execute(f"""REPLACE INTO
                            CostFixed(regions, periods, tech, vintage, data_cost_fixed, data_cost_year, data_curr,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{tech}', {vint}, {cost_fixed}, {curr_year}, '{curr}',
                            '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")



    conn.commit()
    conn.close()