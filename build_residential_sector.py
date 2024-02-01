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
import utils
from setup import config
from matplotlib import pyplot as pp

# Check if database exists or needs to be built
build_db = not os.path.exists(config.database_file)

# Connect to the new database file
conn = sqlite3.connect(config.database_file)
curs = conn.cursor() # Cursor object interacts with the sqlite db

# Instantiate the database if it doesn't exist
if build_db: curs.executescript(open(config.schema_file, 'r').read())

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

if not config.params['skip_dsd']: all_subsectors.aggregate_dsd()

all_subsectors.cleanup()
all_subsectors.aggregate_post()

# Show any plots that have been made
if config.params['clone_to_xlsx']: utils.DatabaseConverter().clone_sqlite_to_excel(config.database_file, config.excel_target_file, excel_template_file=config.excel_template_file)
if config.params['show_plots']: pp.show()

"""
##############################################################
    The following is temporary for buildings sector testing
##############################################################
"""

# Connect to the new database file
conn = sqlite3.connect(config.database_file)
curs = conn.cursor() # Cursor object interacts with the sqlite db

fuel_costs = {
    "NG": 8.847,
    "OIL": 25.163,
    "ELC": 31.944,
    "WOOD": 17.38,
    "LPG": 49.88
}

base_emis = {
    "ON": 16800,
    "AB": 8700,
    "BC": 4300,
    "MB": 1200,
    "SK": 1900,
    "QC": 3100
}

emis = {
    2025: 1,
    2030: 0.8,
    2035: 0.6,
    2040: 0.4,
    2045: 0.2,
    2050: 0
}
            
rep_days = [
    'D001',
    'D009',
    'D045', # Coldest day ON 2020
    'D103',
    'D128',
    'D173',
    'D184' # Hottest day ON 2020
]

seas_tables = [
    'DemandSpecificDistribution'
]

cost_tables = [
    'Cost_Invest',
    'Cost_Fixed',
    'Cost_Variable'
]

# Delete all days but rep days above
curs.execute(f"DELETE FROM time_season")
[curs.execute(f"INSERT OR IGNORE INTO time_season(t_season) VALUES('{day}')") for day in rep_days]

for table in seas_tables:
    curs.execute(f"DELETE FROM {table} WHERE season_name NOT IN (SELECT t_season from time_season)")

curs.execute(f"DELETE FROM SegFrac")
for day in rep_days:
    for h in range(24):
        curs.execute(f"""REPLACE INTO SegFrac(season_name, time_of_day_name, segfrac)
                    VALUES('{day}', '{config.time.loc[h, 'time_of_day']}', {1/(24*7)})""")
        
# Renormalise dsd
for end_use in config.end_use_demands['comm']:
    for region in config.model_regions:
        total_dsd = sum([dsd[0] for dsd in curs.execute(f"""SELECT dsd FROM DemandSpecificDistribution
                                                        WHERE demand_name == '{end_use}' AND regions == '{region}'""").fetchall()])
        curs.execute(f"""UPDATE DemandSpecificDistribution
                    SET dsd = dsd / {total_dsd}
                    WHERE demand_name == '{end_use}' and regions = '{region}'""")

# Add fuel imports and costs
for fuel, cost in fuel_costs.items():
    curs.execute(f"""INSERT OR IGNORE INTO
                technologies(tech, flag, sector, tech_desc)
                VALUES('R_IMP_{fuel}', 'r', 'residential', 'testing dummy')""")
    
    for region in config.model_regions:
        curs.execute(f"""REPLACE INTO
                    Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes, data_flags)
                    VALUES('{region}', 'R_ethos', 'R_IMP_{fuel}', {config.model_periods[0]}, 'R_{fuel}', 1, 'testing dummy', 'TEST')""")
        
        for period in config.model_periods:
            curs.execute(f"""REPLACE INTO
                        CostVariable(regions, periods, tech, vintage, data_cost_variable, data_cost_year, data_curr, data_flags)
                        VALUES('{region}', {period}, 'R_IMP_{fuel}', {config.model_periods[0]}, {cost}, 2020, 'CAD', 'TEST')""")

for region in config.model_regions:
    for period in config.model_periods:
        curs.execute(f"""REPLACE INTO
                    EmissionLimit(regions, periods, emis_comm, emis_limit, emis_limit_units)
                    VALUES('{region}', {period}, "CO2eq", {emis[period]*base_emis[region]}, "ktCO2eq")""")

for table in cost_tables:
    curs.execute(f"""UPDATE {table.replace('_','')}
                 SET {table.lower()} = data_{table.lower()}""")


conn.commit()
conn.close()