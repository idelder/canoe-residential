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
import time
import sqlite3
import numpy as np
from matplotlib import pyplot as pp
from datetime import datetime

print(config.regions.loc['ON', 'ca_station'])

a = np.ones([3,3])
print(a)
np.savetxt('a.csv', a, delimiter=',')
np.loadtxt('a.csv', dtype=float, delimiter=',')
print(a)