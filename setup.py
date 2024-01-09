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

    _instance = None # singleton pattern


    def __new__(cls, *args, **kwargs):

        if isinstance(cls._instance, cls): return cls._instance

        cls._instance = super(config, cls).__new__(cls, *args, **kwargs)
        cls._get_params()

        print('Instantiated setup config.')

        return cls._instance


    def _get_params():
        
        stream = open(config._this_dir + "res_config.yaml", 'r')
        config.params = yaml.load(stream, Loader=yaml.Loader)

        config.model_periods = config.params['model_periods']

        print(config.params)



# Instantiate on import
config()