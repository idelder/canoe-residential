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
import all_subsectors
from datetime import datetime

data = np.array([*([4]*24),*([5]*24),*([6]*24),*([7]*24),*([8]*24),*([12]*24),*([13]*24)]*52+[4]*24)
pp.plot(data)

weekly_avg = [np.mean(data[7*24*w:7*24*w+7*24]) for w in range(52)]
time_of_week = [np.mean(data[h:7*24:52*7*24]/weekly_avg) for h in range(7*24)]

print(time_of_week)

pp.show()