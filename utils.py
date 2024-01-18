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



this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
cache_dir = this_dir + "download_cache/"
input_files = this_dir + 'input_files/'
excel_template = input_files + 'Template spreadsheet (make a copy).xlsx'



# Cleans up strings for filenames, databases, etc.
def string_cleaner(string):

    return ''.join(letter for letter in string if letter in '- /()–' or letter.isalnum())



def string_letters(string):

    return ''.join(letter for letter in string_cleaner(string) if letter not in '123456789')



def clean_index(df):

    df.index = [string_letters(idx) for idx in df.index]



def compr_db_url(region, table_number):

    return str(config.params['nrcan_url']).replace('<y>', str(config.params['nrcan_data_year'])).replace('<r>', region.lower()).replace('<t>', str(table_number))



def get_statcan_table(table, save_as=None, use_cache=True):

    if save_as == None: save_as = f"statcan_{table}.csv"
    if os.path.splitext(save_as)[1] != ".csv": save_as += ".csv"

    if use_cache and os.path.isfile(cache_dir + save_as):

        try:

            df = pd.read_csv(cache_dir + save_as)
            
            print(f"Got Statcan table {table} from local cache.")
            return df
        
        except Exception as e:

            print(f"Could not get Statcan table {table} from local cache. Trying to download instead.")

    # Make a request from the API for the table, returns response status and url for download
    url = f"https://www150.statcan.gc.ca/t1/wds/rest/getFullTableDownloadCSV/{table}/en"
    response = requests.get(url).json()

    # If successful, download the table
    if response['status'] == 'SUCCESS':

        # Download and open the zip file
        filehandle,_ = urllib.request.urlretrieve(response['object'])
        zip_file_object = zipfile.ZipFile(filehandle, 'r')

        # Read the table from inside the zip file
        from_file = zip_file_object.open(f"{table}.csv", "r")

        # Write the table to download cache
        to_file = open(cache_dir + save_as, "wb")
        to_file.write(from_file.read())

        # Close files
        from_file.close()
        to_file.close()
        
        df = pd.read_csv(cache_dir + save_as)

        print(f"Successfully downloaded Statcan table {table}.")
        return df

    else:

        print(f"Request for {table} from Statcan failed. Status: {response['status']}")
        return None
        


# Downloads and handles local caching of data sources
def get_data(url, file_type=None, name=None, use_cache=True, **kwargs):

    # Get the original file name
    if name == None: name = url.split("/")[-1].split("\\")[-1]
    if file_type == None: file_type = url.split(".")[-1]

    file_type = file_type.lower()

    if file_type == "xml": file_type = "json"
    if file_type == "xls": file_type = "xlsx"
    if url.split(".")[-1] != file_type: name = os.path.splitext(name)[0] + "."+file_type
    cache_file = cache_dir + name

    data = None
    if (use_cache and os.path.isfile(cache_file)):
        
        # Get from existing local cache
        if file_type == "csv": data = pd.read_csv(cache_file, index_col=0)
        elif "xl" in file_type: data = pd.read_excel(cache_file, index_col=0)
        elif file_type == "xml": data = json.load(open(cache_file))
        print(f"Got {name} from local cache.")
        
    else:

        # Download from url
        if file_type == "csv": data = pd.read_csv(url, **kwargs)
        elif "xl" in file_type: data = pd.read_excel(url, **kwargs)
        elif file_type == "xml": data = json.dumps(xmltodict.parse(requests.get(url).content))

        # Try to cache downloaded file
        try:
            if not os.path.exists(cache_dir): os.mkdir(cache_dir)

            if file_type == "csv": data.to_csv(cache_file)
            elif "xl" in file_type: data.to_excel(cache_file)
            elif file_type == "xml":
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



def feasible_vintages(period, lifetime, vint_interval=config.params['period_step']):

    vint_0 = period - period % vint_interval

    return [*range(int(vint_0), int(period-lifetime), -int(vint_interval))]
    


class DatabaseConverter:
    
    # Singleton pattern
    _instance = None

    def __new__(cls, *args, **kwargs):

        if isinstance(cls._instance, cls): return cls._instance

        cls._instance = super(DatabaseConverter, cls).__new__(cls, *args, **kwargs)

        print('Instantiated database converter.')

        return cls._instance

    def clone_sqlite_to_excel(self, from_sqlite_file, to_excel_file, excel_template_file=None):
        
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