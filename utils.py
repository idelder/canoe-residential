"""
Various tools
Written by Ian David Elder for the TEMOA Canada / CANOE model
"""


import os
import shutil
from openpyxl import load_workbook
import sqlite3
import pandas as pd
import requests
import xmltodict
import json
from setup import config
import urllib.request
import zipfile
import itertools
import time
import threading
import sys
import numpy as np
from datetime import datetime
from matplotlib import pyplot as pp



this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
cache_dir = this_dir + "download_cache/"
input_files = this_dir + 'input_files/'
excel_template = input_files + 'Template spreadsheet (make a copy).xlsx'

base_year = config.params['base_year']
weather_maps = dict()



# Cleans up strings for filenames, databases, etc.
def string_cleaner(string):

    return ''.join(letter for letter in string if letter in '- /()–' or letter.isalnum())



def string_letters(string):

    return ''.join(letter for letter in string_cleaner(string) if letter not in '123456789')



def clean_index(df):

    df.index = [string_letters(idx).lower() for idx in df.index]



def compr_db_url(region, table_number):

    return str(config.params['nrcan_url']).replace('<y>', str(config.params['base_year'])).replace('<r>', region.lower()).replace('<t>', str(table_number))



def get_statcan_table(table, save_as=None, use_cache=True, **kwargs):

    if save_as == None: save_as = f"statcan_{table}.csv"
    if os.path.splitext(save_as)[1] != ".csv": save_as += ".csv"

    if use_cache and os.path.isfile(cache_dir + save_as):

        try:

            df = pd.read_csv(cache_dir + save_as, index_col=0)
            
            print(f"Got Statcan table {table} from local cache.")
            return df
        
        except Exception as e:

            print(f"Could not get Statcan table {table} from local cache. Trying to download instead.")

    # Make a request from the API for the table, returns response status and url for download
    url = f"https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/{table}/en"
    response = requests.get(url).json()

    # If successful, download the table
    if response['status'] == 'SUCCESS':

        print(f"Downloading Statcan table {table}...")

        # Download and open the zip file
        filehandle,_ = urllib.request.urlretrieve(response['object'])
        zip_file_object = zipfile.ZipFile(filehandle, 'r')

        # Read the table from inside the zip file
        from_file = zip_file_object.open(f"{table}.csv", "r")
        df = pd.read_csv(from_file, **kwargs)
        from_file.close()

        df.to_csv(cache_dir + save_as)

        print(f"Cached Statcan table {table}.")
        return df

    else:

        print(f"Request for {table} from Statcan failed. Status: {response['status']}")
        return None
    


def get_compr_db(region, table_number, first_row=0, last_row=None):

    table = get_data(compr_db_url(region, table_number), skiprows=10)
    table = table.loc[first_row::] if last_row is None else table.loc[first_row:last_row]
    table = table.drop("Unnamed: 0", axis=1).set_index('Unnamed: 1').dropna()
    table.index.name = None
    clean_index(table)

    return table



# Downloads and handles local caching of data sources
def get_data(url, file_type=None, cache_file_type=None, name=None, use_cache=True, **kwargs) -> pd.DataFrame:

    # Get the original file name
    if name == None: name = url.split("/")[-1].split("\\")[-1]
    if file_type == None: file_type = url.split(".")[-1]

    file_type = file_type.lower()

    if cache_file_type == None:
        if file_type == "xml": cache_file_type = "json"
        elif file_type == "xls": cache_file_type = "xlsx"
        else: cache_file_type = file_type
    
    # If file type is different from new file type
    if url.split(".")[-1] != cache_file_type: name = os.path.splitext(name)[0] + "."+cache_file_type
    cache_file = cache_dir + name

    data = None
    if (use_cache and os.path.isfile(cache_file)):
        
        # Get from existing local cache
        if cache_file_type == "csv": data = pd.read_csv(cache_file, index_col=0)
        elif "xl" in cache_file_type: data = pd.read_excel(cache_file, index_col=0)
        elif cache_file_type == "xml": data = json.load(open(cache_file))
        print(f"Got {name} from local cache.")
        
    else:

        print(f"Downloading {name} ...")

        is_done = [False]
        #working_wheel(is_done)

        try:
            # Download from url
            if file_type == "csv": data = pd.read_csv(url, **kwargs)
            elif "xl" in file_type: data = pd.read_excel(url, **kwargs)
            elif file_type == "xml": data = json.dumps(xmltodict.parse(requests.get(url).content))
        except Exception as e:
            print(url)
            print(e)
        finally:
            is_done[0] = True

        # Try to cache downloaded file
        try:
            if not os.path.exists(cache_dir): os.mkdir(cache_dir)

            if cache_file_type == "csv": data.to_csv(cache_file)
            elif "xl" in cache_file_type: data.to_excel(cache_file)
            elif cache_file_type == "xml":
                with open(cache_file, 'w') as outfile: outfile.write(data)
            print(f"Cached {name}.")
        except Exception as e:
            print(e)

    return data



# Gives data quality time-related indicator based on time gap from data
def dq_time(from_year, to_year):
    diff = abs(from_year - to_year)

    data_quality = {
        3: 1,
        6: 2,
        10: 3,
        15: 4
    }

    for key in data_quality.keys():
        if diff <= key: return data_quality[key]
    
    return 5 # greater than 15 years time difference



def stock_vintages(stock_year, lifetime, vint_interval=config.params['period_step']) -> list:

    vint_0 = stock_year - stock_year % vint_interval # first stepped back vint

    # Return any stepped back vintages that are feasible
    vints = list(range(int(vint_0), int(stock_year-lifetime), -int(vint_interval)))
    vints.sort()

    return vints
    


class DatabaseConverter:
    
    # Singleton pattern
    _instance = None

    def __new__(cls, *args, **kwargs):

        if isinstance(cls._instance, cls): return cls._instance

        cls._instance = super(DatabaseConverter, cls).__new__(cls, *args, **kwargs)

        print('Instantiated database converter.')

        return cls._instance

    def clone_sqlite_to_excel(self, from_sqlite_file: str, to_excel_file: str, excel_template_file: str = None):
        
        # Make sure behaviour is understood
        overwrite = input(f"\nAbout to clone {os.path.basename(from_sqlite_file)} "\
                        f"into target {os.path.basename(to_excel_file)}. "\
                        "This may take some time.\n"
                        "Any data in the workbook that is not in the sqlite database will be lost. Proceed? (Y/N):")
        if overwrite.upper() != "Y":
            print("Did not overwrite workbook.")
            return
        
        print(f"\nCloning {os.path.basename(from_sqlite_file)} into target {os.path.basename(to_excel_file)}."\
              "\nThis may take a minute...")

        if not os.path.isfile(to_excel_file):

            # Check that the target file or template file exists
            if (excel_template_file is None):
                print("Target excel file does not yet exist. Must provide a template to copy.")
                return
            
            # Copy template to make target file if target doesn't yet exist
            shutil.copy(excel_template_file, to_excel_file)

        # Load the target workbook
        wb = load_workbook(to_excel_file)

        # Connect to the sqlite from file and get data table names
        conn = sqlite3.connect(from_sqlite_file)
        curs = conn.cursor()
        fetched = curs.execute("""SELECT name FROM sqlite_master WHERE type='table'""").fetchall()

        # Skipping output tables, since this was written for input data
        all_tables = [table[0] for table in fetched if (not table[0].startswith('Output'))]

        for sheet in wb.sheetnames:
            if sheet not in all_tables: print(f"Target sheet {sheet} missing from sqlite database.")

        # Copy tables from sqlite to excel target
        for table_name in all_tables:
            
            if table_name not in wb.sheetnames:
                print(f"Table {table_name} missing from target workbook and was ignored.")
                continue
            
            # Get sqlite column names and all data rows, put into pandas dataframe
            rows = curs.execute(f"SELECT * FROM '{table_name}'")
            sql_cols = [desc[0] for desc in rows.description]
            sql_df = pd.DataFrame(data=rows.fetchall(), columns=sql_cols)

            # Get this table from the target excel workbook and its headers (might not be same as sql)
            ws = wb[table_name]
            xl_headers = [cell.value for cell in ws[1]]

            # Flag a warning if a sqlite column does not have a counterpart in the target excel workbook
            for sql_col in sql_cols:
                if sql_col not in xl_headers: print(f"Sqlite column {sql_col} missing from spreadsheet table {table_name} and was ignored.")
            
            # Prepare a target dataframe matching the target excel workbook template
            xl_df = pd.DataFrame(columns=xl_headers)
            for xl_head in xl_headers:

                # Flag a warning if sqlite table is missing a column that is in the excel workbook
                # This might not be an issue, but maybe that column should be added to the sqlite database
                if xl_head not in sql_cols:
                    print(f"Spreadsheet column {xl_head} missing from sqlite table {table_name}.")
                    continue
                
                # Fill target workbook dataframe with data from sqlite dataframe
                xl_df[xl_head] = sql_df[xl_head]

            # Clear the target excel table
            ws.delete_rows(2, ws.max_row)

            # Refill the target excel table with data from sqlite
            for index, row in xl_df.iterrows():
                ws.append(row.values.tolist())

        wb.save(to_excel_file)



def animate_wheel(is_done):
    for c in itertools.cycle(['|', '/', '-', '\\']):
        time.sleep(0.1)
        if is_done[0]:
            break
        sys.stdout.write('\r' + c)
        sys.stdout.flush()
        time.sleep(0.1)
    sys.stdout.write('\r')
    sys.stdout.flush()



def working_wheel(is_done):
    t = threading.Thread(target=animate_wheel, args=(is_done,))
    t.start()



def weather_map_data(region, us_data: list) -> pd.Series:

    # If the mapper already exists then just use it
    if region in weather_maps.keys(): return apply_weather_map(region, us_data)
        

    ## Otherwise generate the map
    print(f"Generating a weather-based data map from {config.regions.loc[region, 'us_state']} to {region}...")

    # Days in each month of the year. Need because of dodgy index
    days_in_months = [31,28,31,30,31,30,31,31,30,31,30,31]

    # Weather data from the US
    df_us_wth = get_data(
        config.params['weather']['us']['url']
        .replace('<st>', str(int(config.regions.loc[region, 'us_station'])))
        .replace('<y>', str(config.params['weather']['us']['year']))
        , index_col=1, usecols=range(15)
        )
    df_us_wth = df_us_wth.loc[df_us_wth.index.str.contains('53')] # TODO This may not apply to all stations in retrospect...
    df_us = pd.DataFrame(index=range(8760))

    # Clean up us station data
    i=0
    for m in range(12):
        for d in range(days_in_months[m]):
            for h in range(24):
                ms = f"0{m+1}" if m+1 < 10 else str(m+1)
                ds = f"0{d+1}" if d+1 < 10 else str(d+1)
                hs = f"0{h}" if h < 10 else str(h)
                idx = f"2018-{ms}-{ds}T{hs}:53:00"

                for val in ['TMP','DEW']:
                    if idx not in df_us_wth.index: v = pd.NA
                    else:
                        v = float(df_us_wth.loc[idx, val].split(',')[0])/10
                        if v > 50: v = pd.NA
                    df_us.loc[i, val] = v

                i+=1

    # Fill in temperature gaps by chronological linear interpolation
    df_us.interpolate(method='linear', axis='columns', inplace=True)

    # Canadian weather data
    df_ca = get_data(
        config.params['weather']['canada']['url']
        .replace('<st>', str(int(config.regions.loc[region, 'ca_station'])))
        .replace('<r>', region)
        .replace('<y>', str(base_year))
        , encoding='unicode_escape', usecols=range(12)
        ).iloc[0:8760]

    # A 2D matrix map of which US data points to use per Canadian datum
    weather_maps[region] = np.zeros((8760,8760))

    for i, row in df_ca.iterrows():

        # For this datum, boolean vector of relevant data in US data vector
        row_map = 1.0*np.array((row['Temp (°C)'] < df_us['TMP']+1) &
            (row['Temp (°C)'] > df_us['TMP']-1) &
            (row['Dew Point Temp (°C)'] > df_us['DEW']-1) &
            (row['Dew Point Temp (°C)'] < df_us['DEW']+1)).transpose()
        
        # Get the mean of relevant US data and or set NaN if no mappable hours
        row_map *= np.nan if np.sum(row_map) == 0 else 1 / np.sum(row_map)

        # Save these matrix maps per region to save having to repeat the above slow process
        weather_maps[region][i,:] = row_map.copy()


    return apply_weather_map(region, us_data)


def apply_weather_map(region, us_data: list):

    # Canadian data starts as us data mapped to temperature and dew point temperature
    ca_data = pd.Series(np.matmul(weather_maps[region], us_data)).interpolate(method='linear')

    # Then get the day of week of Jan 1 for each year. Monday is 0, Sunday 6
    jan_1_us = datetime.weekday(datetime.fromisoformat(f"{config.params['weather']['us']['year']}-01-01"))
    jan_1_ca = datetime.weekday(datetime.fromisoformat(f"{base_year}-01-01"))

    # Get multipliers for time of the week, hourly
    daily_avg = np.array([np.mean(us_data[24*d:24*d+24]) for d in range(364)])
    weekly_avg = np.array([np.mean(us_data[7*24*w:7*24*w+7*24]) for w in range(52)])
    day_of_week = [np.mean(daily_avg[d:52*7:7]/weekly_avg) for d in range(7)]
    hour_of_day = [np.mean(us_data[h:24*7*52:24]/daily_avg) for h in range(24)]
    time_of_week = [day_of_week[h//24] * hour_of_day[h%24] for h in range(24*7)] # a. hour of day factor times day of week factor. Use this
    #time_of_week = [np.mean(us_data[h:24*7*52:24*7]/weekly_avg) for h in range(24*7)] # b. hour of week factor

    # Shift multipliers to correct day of week based on jan 1 day
    time_of_week_zeroed = time_of_week[-24*jan_1_us::] + time_of_week[0:-24*jan_1_us] # starts on monday
    tow_mults = time_of_week_zeroed[24*jan_1_ca::] + time_of_week_zeroed[0:24*jan_1_ca] # starts on jan 1 day base year
    
    # Take hourly time of week multiplier and stretch out to whole year (8760)
    tow_mults = tow_mults*52 + tow_mults[0:24] # 52 weeks + 1 day in a year
    tow_mults /= np.mean(tow_mults) # normalise

    # Apply time of week multipliers
    ca_data *= tow_mults
    
    # Return mapped data and time-of-week multipliers Mon -> Sun
    return ca_data, time_of_week_zeroed/np.mean(time_of_week_zeroed)