"""
Aggregates data for residential lighting
Written by Ian David Elder for the CANOE model
"""

import utils
import pandas as pd
import os
import numpy as np
import sqlite3
from setup import config

# Shortens lines a bit
statcan_year = config.params['statcan_data_year']
statcan_ref = config.params['statcan_reference']
base_year = config.params['base_year']
nrcan_ref = config.params['nrcan_reference']
aeo_ref = config.params['aeo_updated_reference']
aeo_year = config.params['aeo_data_year']
on_stock_ref = config.params['lighting']['on_stock_ref']
usage_ref = config.params['lighting']['usage_ref']
curr = config.params['aeo_currency']
curr_year = config.params['aeo_currency_year']
conv = config.params['conversion_factors']['lighting']

# Some common variables
in_comm = config.fuel_commodities.loc['electricity']
lighting = config.end_use_demands.loc['lighting']
acf = config.params['lighting']['annual_capacity_factor']

"""
##############################################################
    Non-regional data
##############################################################
"""

# Get provincial data on relative usage of different bulb types from Statcan table 38100048
lgt_usage = utils.get_statcan_table(38100048)
lgt_usage['GEO'] = lgt_usage['GEO'].str.lower()

# Configuration file for lighting technologies, including Ontario shares data from residential end use survey
exs_techs = pd.read_csv(config.input_files + '/existing_lighting_technologies.csv', index_col=0)
aeo_data = pd.read_csv(config.input_files + '/aeo_lighting_data.csv', index_col=0)
aeo_techs = pd.read_csv(config.input_files + '/new_lighting_technologies.csv', index_col=0)



# Gets a value from aeo lighting data
def get_aeo_value(code, metric, vintage):
    
    # Get data from latest preceding vintage
    vints = np.array([int(col) for col in aeo_data.columns if col.isdecimal()])
    if vintage < min(vints): last_vint = vints[0]
    else: last_vint = vints[vints < vintage][-1]
    value = aeo_data.loc[aeo_data['metric']==metric].loc[code, str(last_vint)]

    # If no value for that vintage, take existing stock value
    if pd.isna(value): value = aeo_data.loc[aeo_data['metric']==metric].loc[code, 'existing']
    
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



def aggregate():

    for region in config.model_regions: aggregate_region(region)

    print(f"Lighting data aggregated into {os.path.basename(config.database_file)}\n")



def aggregate_region(region):

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db



    """
    ##############################################################
        Annual Capacity Factor
    ##############################################################
    """

    # ACF mostly arbitrary but affects lifetime
    acf_note = config.params['lighting']['acf_note']
    min_note = acf_note + " 99% of max acf."
    acf_ref = config.params['lighting']['acf_reference']
    acf_data_year = config.params['lighting']['acf_data_year']

    for code, row in aeo_techs.iterrows():

        if not row['include_new']: continue

        for period in config.model_periods:

            dq_time = utils.dq_time(period, acf_data_year)

            curs.execute(f"""REPLACE INTO
                            MinAnnualCapacityFactor(regions, periods, tech, output_comm, min_acf, min_acf_notes,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{row['tech']}', '{lighting['comm']}', {acf*0.99}, '{min_note}',
                            '{acf_ref}', {acf_data_year}, 1, 1, 1, {dq_time}, 1, 1)""")
            curs.execute(f"""REPLACE INTO
                            MaxAnnualCapacityFactor(regions, periods, tech, output_comm, max_acf, max_acf_notes,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{row['tech']}', '{lighting['comm']}', {acf}, '{acf_note}',
                            '{acf_ref}', {acf_data_year}, 1, 1, 1, {dq_time}, 1, 1)""")



    """
    ##############################################################
        Demand
    ##############################################################
    """

    # The major challenge of lighting is estimating existing capacity of lighting types
    # If we had better data for this everything would be fine... but we only have for Ontario
    # So we take stock data for Ontario and index it to usage rates from a Statcan survey per province

    note = (f"{base_year} secondary energy (NRCan, {base_year}) multiplied by average efficacy (efficiency) of existing lighting stock. "
            f"Indexed to projected population (Statcan, {statcan_year})")
    reference = f"{nrcan_ref}; {statcan_ref}"
    
    # Get usage of bulb types for this region relative to Ontario
    # Because we have actual shares data for Ontario (residential end use survey)
    reg_usage = get_usage(region)
    usage_index = reg_usage / on_usage

    # Calculate regional shares by indexing ontario shares to Statcan usage survey
    reg_shares = exs_techs.rename({'on_share_sf':'share_sf', 'on_share_mf':'share_mf'}, axis=1)
    for code, shares in reg_shares.iterrows():
        statcan_cat = exs_techs.loc[code, 'statcan_category']
        shares[['share_sf', 'share_mf']] *= usage_index.loc[statcan_cat]
    for col in reg_shares[['share_sf', 'share_mf']].columns: reg_shares[col] /= reg_shares[col].sum() # reset to sum 100%

    # Table 14: Total Households by Building Type and Energy Source
    t14 = utils.get_compr_db(region, 14, 9, 12)[base_year] / 100 # % shares
    
    # Aggregate subcategories of housing into single-family and multi-family
    for cat, subcats in config.params['housing_categories'].items():
        subcats = subcats.split('+')
        t14[cat] = sum([t14[subcat] for subcat in subcats])
        t14 = t14.drop(subcats)
    
    # Mapping existing stock AEO data to existing technologies
    for code, _exs in exs_techs.iterrows():
        data = aeo_data.loc[code].pivot_table(values='existing', index='code', columns='metric')
        for metric in data.columns: exs_techs.loc[code, metric] = data[metric].iloc[0]
    
    # Unit conversion
    exs_techs['efficacy'] *= conv['efficacy'] # efficacy lm/W to Glmy/PJ
    exs_techs['cost_maintain'] *= conv['cost'] # $/klmy to $/Glmy
    exs_techs['lamp_life'] = round(exs_techs['lamp_life'] * conv['lifetime'] / acf)

    # Finally, calculate the average efficacy of existing lighting stock, indexed to shares of single-family vs multi-family housing
    exs_eff = 0 # Glmy/PJ
    for code_exs, row_exs in reg_shares.iterrows():
        reg_shares.loc[code_exs, 'share_tot'] = np.dot(row_exs[['share_sf','share_mf']].values, t14.values)
        exs_eff += exs_techs.loc[code_exs, 'efficacy'] * reg_shares.loc[code_exs, 'share_tot']

    # Table 3: Lighting Secondary Energy Use and GHG Emissions
    sec = utils.get_compr_db(region, 3, 1, 1)[base_year].iloc[0]

    # Demand is secondary energy times 2018 average lighting stock efficacy, indexed to population growth
    pop = config.populations[region]
    dem = exs_eff * sec * pop / pop.loc[base_year]

    # Write demand to database
    for period in config.model_periods:
        curs.execute(f"""REPLACE INTO
                    Demand(regions, periods, demand_comm, demand, demand_units, demand_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', {period}, '{lighting['comm']}', {dem.loc[period].iloc[0]}, '({lighting['dem_unit']})', '{note}',
                    '{reference}', {base_year}, 3, 3, 1, {utils.dq_time(period, base_year)}, 3, 1)""")
        


    """
    ##############################################################
        Existing stock data
    ##############################################################
    """

    # Existing capacity in Glmy at time of first model period when indexed to population growth
    exs_techs['existing_capacity'] = reg_shares['share_tot'] * dem.loc[config.model_periods[0]].iloc[0] / acf
    
    # Distribute existing capacities over feasible past vintages
    for code, exs in exs_techs.iterrows():

        lifetime = exs['lamp_life']
        vints, weights = utils.stock_vintages(base_year, lifetime)
        if max(vints) + lifetime <= config.model_periods[0]: continue # this technology never reaches the first model period

        aeo_note = f"Assumed same as {aeo_techs.loc[code, 'tech']}."
        
        tech_desc = f"lighting - {exs.loc['description']}"
        curs.execute(f"""REPLACE INTO
                    technologies(tech, flag, sector, tech_desc, reference)
                    VALUES('{exs['tech']}', 'p', 'residential', '{tech_desc}', '{nrcan_ref}')""")
        curs.execute(f"""REPLACE INTO
                    LifetimeTech(regions, tech, life, life_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', '{exs['tech']}', {lifetime}, '(y) {aeo_note}',
                    '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")

        for period in config.model_periods:
            if max(vints) + lifetime <= period: continue

            dq_time = utils.dq_time(period, acf_data_year)
            curs.execute(f"""REPLACE INTO
                        MinAnnualCapacityFactor(regions, periods, tech, output_comm, min_acf, min_acf_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', {period}, '{exs['tech']}', '{lighting['comm']}', {acf*0.99}, '{min_note}',
                        '{acf_ref}', {acf_data_year}, 1, 1, 1, {dq_time}, 1, 1)""")
            curs.execute(f"""REPLACE INTO
                        MaxAnnualCapacityFactor(regions, periods, tech, output_comm, max_acf, max_acf_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', {period}, '{exs['tech']}', '{lighting['comm']}', {acf}, '{acf_note}',
                        '{acf_ref}', {acf_data_year}, 1, 1, 1, {dq_time}, 1, 1)""")

        # Some lighting techs didn't come around that long ago so restrict the oldest vintage
        if not pd.isna(exs['oldest_vint']): vints = [vint for vint in vints if vint >= exs['oldest_vint']]

        # Write existing data to database
        for v in range(len(vints)):

            vint = vints[v]
            weight = weights[v]

            if vint + lifetime <= config.model_periods[0]: continue

            note = (f"Ontario existing stock of residential bulb types by housing type (IESO, 2018) "
                    f"multiplied by housing stock by type (NRCan, {base_year}). "
                    f"Indexed to relative usage of bulb types by province versus Ontario (Statcan, 2018) "
                    f"and to projected population (Statcan, {statcan_year}).")
            reference = f"{on_stock_ref}; {nrcan_ref}; {usage_ref}; {statcan_ref}"
            curs.execute(f"""REPLACE INTO
                        ExistingCapacity(regions, tech, vintage, exist_cap, exist_cap_units, exist_cap_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{exs['tech']}', {vint}, {exs['existing_capacity'] * weight}, '({lighting['cap_unit']})', '{note}',
                        '{reference}', {2018}, 4, 2, 1, {utils.dq_time(config.model_periods[0], 2018)}, 3, 1)""")
            
            curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{in_comm['comm']}', '{exs['tech']}', {vint}, '{lighting['comm']}', {exs['efficacy']}, '({lighting['dem_unit']}/{in_comm['unit']}) {aeo_note}',
                        '{aeo_ref}', {2018}, 1, 1, 1, 1, 3, 1)""")
            
            for period in config.model_periods:
                if vint > period or vint + lifetime <= period: continue

                curs.execute(f"""REPLACE INTO
                            CostFixed(regions, periods, tech, vintage, cost_fixed_units, cost_fixed_notes, data_cost_fixed, data_cost_year, data_curr,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{exs['tech']}', {vint}, '(M$/{lighting['cap_unit']}.y)', '{aeo_note}', {exs['cost_maintain']}, {curr_year}, '{curr}',
                            '{aeo_ref}', {aeo_year}, 2, 1, 1, 1, 3, 3)""")
    


    """
    ##############################################################
        New stock data
    ##############################################################
    """

    for code, aeo in aeo_techs.iterrows():

        if not aeo['include_new']: continue

        tech_desc = f"lighting - {aeo['description']}"
        curs.execute(f"""REPLACE INTO
                        technologies(tech, flag, sector, tech_desc, reference)
                        VALUES('{aeo['tech']}', 'p', 'residential', '{tech_desc}', '{aeo_ref}')""")

        # Vintages for new stock are model periods
        for vint in config.model_periods:
            
            # Lifetime from aeo data converted from hours to years using the annual capacity factor and rounded
            lifetime = round(get_aeo_value(code, 'lamp_life', vint) * conv['lifetime'] / acf)

            ## LifetimeProcess
            # Using lifetime process because some bulb lives might improve over model periods in aeo data
            note = f"(y) Lamp life in hours (AEO, {aeo_year}) divided by annual capacity factor (DOE, 2012)."
            reference = f"{aeo_ref}; {acf_ref}"
            curs.execute(f"""REPLACE INTO
                        LifetimeProcess(regions, tech, vintage, life_process, life_process_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{aeo['tech']}', {vint}, {lifetime}, '{note}',
                        '{reference}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")
            
            ## Efficiency
            efficacy = conv['efficacy'] * get_aeo_value(code, 'efficacy', vint)
            curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{in_comm['comm']}', '{aeo['tech']}', {vint}, '{lighting['comm']}', {efficacy}, '({lighting['dem_unit']}/{in_comm['unit']})',
                        '{aeo_ref}', {2018}, 1, 1, 1, 1, 3, 1)""")
            
            ## CostInvest
            cost_invest = conv['cost'] * get_aeo_value(code, 'cost_install', vint)
            curs.execute(f"""REPLACE INTO
                        CostInvest(regions, tech, vintage, cost_invest_units, data_cost_invest, data_cost_year, data_curr,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{aeo['tech']}', {vint}, '(M$/{lighting['cap_unit']})', {cost_invest}, {curr_year}, '{curr}',
                        '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")
            
            for period in config.model_periods:
                
                # Can't pay for a technology if it can't exist
                if period < vint or vint + lifetime <= period: continue
                
                ## CostFixed
                cost_fixed = conv['cost'] * get_aeo_value(code, 'cost_maintain', vint)
                curs.execute(f"""REPLACE INTO
                            CostFixed(regions, periods, tech, vintage, cost_fixed_units, data_cost_fixed, data_cost_year, data_curr,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{aeo['tech']}', {vint}, '(M$/{lighting['cap_unit']}.y)', {cost_fixed}, {curr_year}, '{curr}',
                            '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")



    conn.commit()
    conn.close()



if __name__ == "__main__":
    
    aggregate()