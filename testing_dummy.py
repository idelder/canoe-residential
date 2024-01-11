"""
For testing code snippets
"""

import pandas as pd
import utils
from setup import config

vints = utils.feasible_vintages(2022, 5, 22)

print(vints)

print(config.model_periods[0])

print(config.technologies.loc[config.technologies['subsector'].str.contains('space heating')])