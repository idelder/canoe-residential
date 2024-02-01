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

# Connect to the new database file
conn = sqlite3.connect(config.database_file)
curs = conn.cursor() # Cursor object interacts with the sqlite db

all_tables = [fetch[0] for fetch in curs.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
t_tables = [table for table in all_tables if 'tech' in [description[0] for description in curs.execute(f"SELECT * FROM '{table}'").description]]
tr_tables = [table for table in t_tables if 'regions' in [description[0] for description in curs.execute(f"SELECT * FROM '{table}'").description]]

print(tr_tables)