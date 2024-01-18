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
nrcan_year = config.params['nrcan_data_year']
nrcan_ref = config.params['nrcan_reference']
statcan_year = config.params['statcan_data_year']
statcan_ref = config.params['statcan_reference']
fuel_commodities = config.fuel_commodities
nrcan_techs = config.nrcan_techs

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

# Gets relative usage rates of bulb types for a province from Statcan table 38100048
def get_usage(region):

    # Just filtering and pivoting the table to show bulb types as rows and years as columns
    usage = lgt_usage.loc[(lgt_usage['GEO'] == config.regions.loc[region, 'description'])][['Type of energy-saving light','REF_DATE','VALUE']].set_index('Type of energy-saving light')
    usage = usage.pivot_table(values='VALUE', index=usage.index, columns='REF_DATE', aggfunc='first')

    # The residential end use survey was 2018 so interpolate between 2017/2019
    return (usage[2017] + usage[2019])/2

# Ontario usage as a baseline
on_usage = get_usage('on')



def aggregate(region):

    # Connect to the new database file
    conn = sqlite3.connect(database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    """
    ##############################################################
        Demand
    ##############################################################
    """
    
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
    for tech, row in exs_techs.iterrows():
        aeo_data = aeo_techs.loc[(aeo_techs['description'].str.contains(row['description'])) & (~pd.isna(aeo_techs['existing']))]\
                            .pivot_table(values='existing', index='tech', columns='metric')
        for metric in aeo_data.columns: exs_techs.loc[tech, metric] = aeo_data[metric].values[0]
    
    # Unit conversion of efficacy lm/W to Glmy/PJ
    exs_techs['efficacy'] *= config.params['conversion_factors']['lighting']['efficacy'] # lm/W to Glmy/PJ
    exs_techs['lamp_life'] = round(exs_techs['lamp_life'] * config.params['conversion_factors']['lighting']['lifetime'] / config.params['lighting_acf']) # hours to years

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
                    VALUES('{region}', {period}, '{config.params['demand_commodities']['lighting']}', {float(dem.loc[period])}, 'Glm', 'TODO',
                    'TODO', {2018}, 1, 1, 1, {utils.dq_time(period, 2018)}, 1, 1)""")
        


    """
    ##############################################################
        Existing Capacity
    ##############################################################
    """

    # Existing capacity in Glmy at time of first model periods as indexed by population growth
    exs_caps = reg_shares['share_tot'] * float(dem.loc[config.model_periods[0]] / config.params['lighting_acf'])
    
    # Distribute existing capacities over feasible past vintages
    for tech, exs_cap in exs_caps.items():

        life = exs_techs.loc[tech, 'lamp_life']
        vints = utils.feasible_vintages(config.model_periods[0], life)

        # LEDs didn't come around that long ago sp cap the oldest vintage
        if not pd.isna(exs_techs.loc[tech, 'oldest_vint']): vints = [vint for vint in vints if vint >= exs_techs.loc[tech, 'oldest_vint']]
        
        # Divide between feasible vintages
        exs_cap /= len(vints)

        # Write existing capacities to database
        for vint in vints:
            curs.execute(f"""REPLACE INTO
                        ExistingCapacity(regions, tech, vintage, exist_cap, exist_cap_units, exist_cap_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {vint}, {exs_cap}, 'Glm', 'TODO',
                        'TODO', {2018}, 4, 2, 1, {utils.dq_time(config.model_periods[0], 2018)}, 3, 1)""")
            


    """
    ##############################################################
        Efficiency
    ##############################################################
    """

    #efficacy

    """
    ##############################################################
        Annual Capacity Factor
    ##############################################################
    """

    # lighting_acf

    conn.commit()
    conn.close()