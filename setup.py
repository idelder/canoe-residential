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

    _instance = None # singleton pattern


    def __new__(cls, *args, **kwargs):

        if isinstance(cls._instance, cls): return cls._instance

        cls._instance = super(config, cls).__new__(cls, *args, **kwargs)
        cls._get_params(cls)

        print('Instantiated setup config.')

        return cls._instance


    def _get_params(cls):
        
        stream = open(config._this_dir + "res_config.yaml", 'r')
        config.params = dict(yaml.load(stream, Loader=yaml.Loader))

        config.model_periods = list(config.params['model_periods'])
        config.technologies = pd.read_csv(config._input_files + 'technologies.csv', index_col=0)


    def _get_aeo_data(cls):

        config.aeo_res_class = pd.read_excel(config._input_files + 'AEO2023_Reference_case_RDM_technology_menu_rsmess.xlsx', sheet_name='RSCLASS', skiprows=18, nrows=31).dropna()
        config.aeo_res_equip = pd.read_excel(config._input_files + 'AEO2023_Reference_case_RDM_technology_menu_rsmess.xlsx', sheet_name='RSMEQP', skiprows=21, nrows=1084).dropna()



# Instantiate on import
config()