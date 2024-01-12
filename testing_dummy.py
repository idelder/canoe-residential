"""
For testing code snippets
"""

from setup import config

aeo_res_equip = config.aeo_res_equip
df = aeo_res_equip.loc[(aeo_res_equip['First Year']<=2025)
                        & (aeo_res_equip['Last Year']>=2025)
                        & ((aeo_res_equip['Census Division'] == 1)
                           | (aeo_res_equip['Census Division'] == 11))]
df.to_csv('test.csv')