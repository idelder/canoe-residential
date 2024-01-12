"""
Gets generic data that may be needed for other subsector aggregation
"""

from setup import config
import utils
import os
import pandas as pd
from scipy.special import gamma
import sqlite3


this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
input_files = this_dir + 'input_files/'
schema_file = this_dir + "canoe_schema.sql"
database_file = this_dir + "residential.sqlite"


# Shortens lines a bit
aeo_techs = config.aeo_techs
aeo_res_class = config.aeo_res_class
aeo_res_equip = config.aeo_res_equip
aeo_ref = config.params['aeo_reference']
aeo_year = config.params['aeo_data_year']


def aggregate():

    # Connect to the new database file
    conn = sqlite3.connect(database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    """
    ##############################################################
        Commodities
    ##############################################################
    """

    for desc, comm in config.params['fuel_commodities'].items():
        curs.execute(f"""REPLACE INTO
                    commodities(comm_name, flag, comm_desc)
                    VALUES('{comm}', 'p', '(PJ) residential {desc}')""")
    for desc, comm in config.params['demand_commodities'].items():
        curs.execute(f"""REPLACE INTO
                    commodities(comm_name, flag, comm_desc)
                    VALUES('{comm}', 'd', '(PJ) demand for residential {desc}')""")

    """
    ##############################################################
        Technologies, LifetimeTech, tech-vintage pairs
    ##############################################################
    """

    config.lifetimes = {}

    ##############################################################
    # AEO data (new)
    ##############################################################

    note = 'Average of Weibull distribution'

    for tech, row in aeo_techs.iterrows():
        
        tech_desc = f"{row.loc['end_uses']} - {row.loc['description']}"
        curs.execute(f"""REPLACE INTO
                        technologies(tech, flag, sector, tech_desc, reference)
                        VALUES('{tech}', 'p', 'residential', '{tech_desc}', '{aeo_ref}')""")

        aeo_class = row.loc['aeo_class']
        end_use_ids = row.loc['end_use_ids'].split('+')

        # Get lifetime from mean of weibull distribution
        weibull_k = aeo_res_class.loc[aeo_res_class['End Use'] == int(end_use_ids[0])].loc[aeo_class, 'Weibull K']
        weibull_l = aeo_res_class.loc[aeo_res_class['End Use'] == int(end_use_ids[0])].loc[aeo_class, 'Weibull λ']

        lifetime = round(weibull_l * gamma(1 + 1/weibull_k)) # mean of weibull distribution

        # Add feasible vintages to config dictionary
        config.tech_vints[tech] = config.model_periods

        ## LifetimeTech
        for region in config.regions.index:
            curs.execute(f"""REPLACE INTO
                        LifetimeTech(regions, tech, life, life_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {lifetime}, '{note}',
                        '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 1, 1)""")

    ##############################################################
    # NRCan data (existing)
    ##############################################################

    for tech, row in config.nrcan_techs.iterrows():

        tech_desc = f"{row.loc['end_use']} - {row.loc['description']}"
        curs.execute(f"""REPLACE INTO
                        technologies(tech, flag, sector, tech_desc, reference)
                        VALUES('{tech}', 'p', 'residential', '{tech_desc}', '{aeo_ref}')""")

        # Get equivalent future tech to this existing tech to pull AEO data
        aeo_class = row.loc['aeo_class']
        equiv_tech = aeo_techs.loc[aeo_techs['aeo_class']==aeo_class].index.values[0]
        end_use_ids = aeo_techs.loc[equiv_tech, 'end_use_ids'].split('+')
        
        # Get lifetime from mean of weibull distribution
        weibull_k = aeo_res_class.loc[aeo_res_class['End Use'] == int(end_use_ids[0])].loc[aeo_class, 'Weibull K']
        weibull_l = aeo_res_class.loc[aeo_res_class['End Use'] == int(end_use_ids[0])].loc[aeo_class, 'Weibull λ']

        lifetime = round(weibull_l * gamma(1 + 1/weibull_k)) # mean of weibull distribution

        # Add feasible vintages to config dictionary
        config.tech_vints[tech] = utils.feasible_vintages(config.model_periods[0], config.params['period_step'], lifetime)

        note = f"Assumed same as {equiv_tech}"

        for region in config.regions.index:
            curs.execute(f"""REPLACE INTO
                        LifetimeTech(regions, tech, life, life_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {lifetime}, '{note}',
                        '{aeo_ref}', {aeo_year}, 2, 1, 1, 1, 1, 2)""")
            
    conn.commit()
    conn.close()
            


def aggregate_region(region):

    # Connect to the new database file
    conn = sqlite3.connect(database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    """
    ##############################################################
        Efficiency, CostInvest
    ##############################################################
    """

    cens_div = config.regions.loc[region, 'us_census_div']

    note = "(PJ/PJ)"
    curr = config.params['aeo_currency']
    curr_year = config.params['aeo_currency_year']

    # Narrow down the dataframe with each nested section to speed things up
    df0 = aeo_res_equip.loc[(aeo_res_equip['Census Division'] == cens_div) | (aeo_res_equip['Census Division'] == 11)]


    # All technologies from aeo technologies input csv
    for tech, row in aeo_techs.iterrows():

        aeo_class = row['aeo_class']
        aeo_equip = row['aeo_equip']

        in_comm = config.params['fuel_commodities'][row['input_comm']]

        if type(df0) is pd.DataFrame: df1 = df0.loc[aeo_equip]
        elif type(df0) is pd.Series: df1 = df0 # only one row remaining

        end_uses = row.loc['end_uses'].split("+")
        end_use_ids = row.loc['end_use_ids'].split("+")
        eff_metric = aeo_res_class.loc[aeo_class, 'Efficiency Metric']
        if type(eff_metric) is pd.Series: eff_metric = eff_metric.values[0]


        # All future periods are valid vintages
        for vint in config.model_periods:
            
            if type(df1) is pd.DataFrame:
                df2 = df1.loc[(df1['First Year']<=vint) & (vint<=df1['Last Year'])]
                cost_invest = df2.loc[df2['New Construction Cost'] != 0]['New Construction Cost'].values[0]
            elif type(df1) is pd.Series:
                df2 = df1 # only one row remaining
                cost_invest = df2['New Construction Cost']

            ## CostInvest
            curs.execute(f"""REPLACE INTO
                        CostInvest(regions, tech, vintage, data_cost_invest, data_cost_year, data_curr,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {vint}, {cost_invest}, {curr_year}, '{curr}',
                        '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 1, 1)""")
            
            ## CostFixed
            cost_fixed = row['cost_fixed']
            if cost_fixed != 0:
                for period in config.model_periods:
                    curs.execute(f"""REPLACE INTO
                                CostFixed(regions, periods, tech, vintage, data_cost_fixed, data_cost_year, data_curr,
                                reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                                VALUES('{region}', {period}, '{tech}', {vint}, {cost_fixed}, {curr_year}, '{curr}',
                                '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 1, 1)""")


            # For each end use the technology supplies (heat pumps do heating and cooling)
            for i in range(len(end_uses)):

                end_use = end_uses[i]
                end_use_id = end_use_ids[i]

                out_comm = config.params['demand_commodities'][end_use]
                
                if type(df2) is pd.DataFrame: eff = df2.loc[(df2['End Use'] == int(end_use_id))]['Efficiency'].values[0]
                elif type(df2) is pd.Series: eff = df2['Efficiency'] # only one row remaining

                ## Efficiency
                curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{in_comm}', '{tech}', {vint}, '{out_comm}', {eff}, '{eff_metric}',
                        '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 1, 1)""")

            
    conn.commit()
    conn.close()