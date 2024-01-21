"""
For testing code snippets
"""

import pandas as pd
from setup import config
import requests
import utils
import os
import urllib.request
import zipfile

# Table 31: Appliance Stock by Appliance Type and Energy Source
t31_elc_stk = utils.get_compr_db("ON", 31, 20, 25)
t31_ng_stk = utils.get_compr_db("ON", 31, 38, 39)

print(config.nrcan_techs)
print(t31_elc_stk)
print(t31_ng_stk)