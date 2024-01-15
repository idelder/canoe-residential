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
import aggregate_generic
import aggregate_space_heating

this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
input_config = this_dir + 'input_config/'
schema_file = this_dir + "canoe_schema.sql"
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

aggregate_generic.aggregate()
aggregate_generic.aggregate_region("ON")
aggregate_space_heating.aggregate("ON")
aggregate_generic.aggregate_post()