"""
For testing code snippets
"""

import pandas as pd
from setup import config
import utils
import os

this_dir = os.path.realpath(os.path.dirname(__file__))

emis_fact = pd.read_excel("input_data/ghg-emission-factors-hub.xlsx", skiprows=13, nrows=76, index_col=2)[['CO2 Factor', 'CH4 Factor', 'N2O Factor']].iloc[1::].dropna()
emis_fact = emis_fact[pd.to_numeric(emis_fact['CO2 Factor'], errors='coerce').notnull()]
for fact in emis_fact.columns: emis_fact[fact] *= config.params['epa_conversion_factors'][fact.strip(' Factor')] * config.params['gwp'][fact.strip(' Factor')]
emis_fact['ktCO2eq/PJ'] = emis_fact.sum(axis=1)

#emis_fac *= config.params['epa_unit_fac']
emis_fact.to_csv(this_dir+'/test.csv')
print(emis_fact.head(100))