"""
Builds residential sector database
Written by Ian David Elder for the CANOE model
"""

import pandas as pd
import sqlite3
import os
import utils
from scipy.special import gamma
from setup import config
import all_subsectors
import space_heating
import space_cooling
import water_heating
import lighting
#import appliances

this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
input_files = this_dir + 'input_files/'
schema_file = input_files + "canoe_schema.sql"
database_file = this_dir + "residential.sqlite"

# Check if database exists or needs to be built
build_db = not os.path.exists(database_file)

# Connect to the new database file
conn = sqlite3.connect(database_file)
curs = conn.cursor() # Cursor object interacts with the sqlite db

# Instantiate the database if it doesn't exist
if build_db: curs.executescript(open(schema_file, 'r').read())

conn.commit()
conn.close()

all_subsectors.aggregate()
all_subsectors.aggregate_region("on")
space_heating.aggregate("on")
space_cooling.aggregate("on")
water_heating.aggregate("on")
lighting.aggregate("on")
# appliances.aggregate("on")
all_subsectors.aggregate_post()
all_subsectors.aggregate_region_post("on")