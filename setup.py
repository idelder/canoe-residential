"""
Sets up configuration for buildings sector aggregation
Written by Ian David Elder for the CANOE model
"""

import os
import sqlite3
import pandas as pd
import yaml


class config:

    # File locations
    _this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
    _input_files = _this_dir + 'input_files/'

    tech_vints = {}
    lifetimes = {}

    _instance = None # singleton pattern


    def __new__(cls, *args, **kwargs):

        if isinstance(cls._instance, cls): return cls._instance

        cls._instance = super(config, cls).__new__(cls, *args, **kwargs)
        cls._get_params(cls)
        cls._get_aeo_data(cls)

        print('Instantiated setup config.')

        return cls._instance


    def _get_params(cls):
        
        stream = open(config._input_files + "res_config.yaml", 'r')
        config.params = dict(yaml.load(stream, Loader=yaml.Loader))

        config.model_periods = list(config.params['model_periods'])
        config.aeo_techs = pd.read_csv(config._input_files + 'aeo_technologies.csv', index_col=0)
        config.nrcan_techs = pd.read_csv(config._input_files + 'nrcan_technologies.csv', index_col=0)
        config.regions = pd.read_csv(config._input_files + 'regions.csv', index_col=0)
        config.fuel_commodities = pd.read_csv(config._input_files + 'fuels.csv', index_col=0)

        config.all_regions = list(config.regions.index.drop('ALL'))
        config.all_techs = [*config.aeo_techs.index.values, *config.nrcan_techs.index.values]

        config.populations = dict()
        for region in config.all_regions: config.populations[region] = pd.read_excel(config._input_files + "/population.xlsx", sheet_name=region, index_col=0)


    def _get_aeo_data(cls):

        config.aeo_res_class = pd.read_excel(config._input_files + 'AEO2023_Reference_case_RDM_technology_menu_rsmess.xlsx',
                                             sheet_name='RSCLASS', skiprows=18, nrows=31, index_col=20).iloc[1:,1:20]
        config.aeo_res_equip = pd.read_excel(config._input_files + 'AEO2023_Reference_case_RDM_technology_menu_rsmess.xlsx',
                                             sheet_name='RSMEQP', skiprows=21, nrows=1084, index_col=28).iloc[2:,1:28]



# Instantiate on import
config()