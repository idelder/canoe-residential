"""
Builds residential sector database
Written by Ian David Elder for the CANOE model
"""

import pandas as pd
import os
import math
import scipy as sp
import numpy as np

this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
cache_dir = this_dir + "data_cache/"
res_menu = cache_dir + "AEO2023_Reference_case_RDM_technology_menu_rsmess.xlsx"

# Collecing general class-based technology data from AEO2023 residential technology menu
df_class = pd.read_excel(res_menu, sheet_name="RSCLASS", skiprows=18)
df_class = df_class.iloc[1:-1] # skip label row
df_class = df_class.loc[df_class['Equipment Class Name'].notna()] # remove empty rows
df_class['Lifetime'] = df_class["Weibull λ"] * sp.special.gamma(list(1 + 1 / df_class['Weibull K'])) # average lifetime from weibull dist

# Collecting technology data from residential technology menu
# 1. Current standard
# 2. Typical
# 3. ENERGY STAR
# 4. High
df_equip = pd.read_excel(res_menu, sheet_name='RSMEQP', skiprows=21, index_col=0)
df_equip = df_equip.iloc[2:-1] # skip two label rows
df_equip = df_equip.loc[df_equip['Tech Name'].notna()] # remove empty rows

print(df_equip.loc[(df_equip['Tech Name'].str.contains('4')) & (df_equip['Census Division'] == 3)])

"""
##############################################################
    Space Heating
##############################################################
"""

