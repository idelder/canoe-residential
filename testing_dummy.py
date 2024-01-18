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

# Get provincial data on relative usage of different bulb types from Statcan table 38100048
lgt_usage = utils.get_statcan_table(38100048)
lgt_usage['GEO'] = lgt_usage['GEO'].str.lower()

print(lgt_usage)