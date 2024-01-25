"""
Builds residential sector database
Written by Ian David Elder for the CANOE model
"""

import sqlite3
import os
import all_subsectors
import space_heating
import space_cooling
import water_heating
import lighting
import appliances
from setup import config
from matplotlib import pyplot as pp

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

## Aggregate subsectors
all_subsectors.aggregate()
for region in config.model_regions:
    all_subsectors.aggregate_region(region)
    space_heating.aggregate(region)
    space_cooling.aggregate(region)
    water_heating.aggregate(region)
    lighting.aggregate(region)
    appliances.aggregate(region)
    all_subsectors.aggregate_region_post(region)
all_subsectors.aggregate_post()

pp.show()