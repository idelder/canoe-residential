"""
Aggregates residential non-subsector-specific data
Written by Ian David Elder for the CANOE model
"""

from setup import config
import utils
import os
from datetime import datetime
import pandas as pd
from scipy.special import gamma
import sqlite3
import numpy as np
from matplotlib import pyplot as pp

# Shortens lines a bit
nrcan_techs = config.nrcan_techs
aeo_techs = config.aeo_techs
aeo_res_class = config.aeo_res_class
aeo_res_equip = config.aeo_res_equip
fuel_commodities = config.fuel_commodities
end_use_demands = config.end_use_demands
aeo_ref = config.params['aeo_reference']
aeo_year = config.params['aeo_data_year']
base_year = config.params['base_year']
nrcan_ref = config.params['nrcan_reference']
statcan_ref = config.params['statcan_reference']
conversion_factors = config.params['conversion_factors']

weather_maps = dict() # 2D matrix maps from us data to canadian data



# For non-regional aggregation
def aggregate():

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db


    """
    ##############################################################
        Basic parameters
    ##############################################################
    """

    for h, row in config.time.iterrows():
        curs.execute(f"""REPLACE INTO
                     time_season(t_season)
                     VALUES('{row['season']}')""")
        curs.execute(f"""REPLACE INTO
                     time_of_day(t_day)
                     VALUES('{row['time_of_day']}')""")
        curs.execute(f"""REPLACE INTO
                     SegFrac(season_name, time_of_day_name, segfrac)
                     VALUES('{row['season']}', '{row['time_of_day']}', {1/8760})""")
        
    for period in [*config.model_periods, config.model_periods[-1] + config.params['period_step']]:
        curs.execute(f"""REPLACE INTO
                     time_periods(t_periods, flag)
                     VALUES({period}, 'f')""")
        
    for label, description in {'f': 'future', 'e': 'existing'}.items():
        curs.execute(f"""INSERT OR IGNORE INTO
                     time_period_labels(t_period_labels, t_period_labels_desc)
                     VALUES('{label}', '{description}')""")

    for region, row in config.regions.iterrows():
        if row['include']:
            curs.execute(f"""REPLACE INTO
                        regions(regions, region_note)
                        VALUES('{region}', '{row['description']}')""")
    
    curs.execute(f"DELETE FROM GlobalDiscountRate")
    curs.execute(f"""REPLACE INTO
                GlobalDiscountRate(rate)
                VALUES({config.params['global_discount_rate']})""")


    """
    ##############################################################
        Commodities
    ##############################################################
    """

    for fuel, row in fuel_commodities.iterrows():
        curs.execute(f"""REPLACE INTO
                    commodities(comm_name, flag, comm_desc)
                    VALUES('{row['comm']}', 'p', '(PJ) residential {fuel}')""")
    for desc, row in end_use_demands.iterrows():
        curs.execute(f"""REPLACE INTO
                    commodities(comm_name, flag, comm_desc)
                    VALUES('{row['comm']}', 'd', '(PJ) demand for residential {desc}')""")
        
    # CO2-equivalent emission commodity
    curs.execute(f"""REPLACE INTO
                commodities(comm_name, flag, comm_desc)
                VALUES('{config.params['emission_commodity']}', 'e', '(ktCO2eq) CO2-equivalent emissions')""")



    """
    ##############################################################
        Technologies, lifetimes, tech-vintage pairs
    ##############################################################
    """

    ##############################################################
    # AEO data (new)
    ##############################################################

    for tech, row in aeo_techs.iterrows():
        
        tech_desc = f"{row.loc['end_uses']} - {row.loc['description']}"
        curs.execute(f"""REPLACE INTO
                        technologies(tech, flag, sector, tech_desc, reference)
                        VALUES('{tech}', 'p', 'residential', '{tech_desc}', '{aeo_ref}')""")

        aeo_class = row.loc['aeo_class']

        # Get lifetime from mean of weibull distribution
        weibull_k = aeo_res_class.loc[aeo_class, 'Weibull K']
        weibull_l = aeo_res_class.loc[aeo_class, 'Weibull λ']

        # There are double ups of class for heat pumps because two end uses
        if type(weibull_k) is pd.Series: weibull_k = weibull_k.values[0]
        if type(weibull_l) is pd.Series: weibull_l = weibull_l.values[0]

        lifetime = round(weibull_l * gamma(1 + 1/weibull_k)) # mean of weibull distribution

        # Add lifetimes and feasible vintages to config dictionaries
        config.lifetimes[tech] = lifetime
        config.tech_vints[tech] = config.model_periods


    ##############################################################
    # NRCan data (existing)
    ##############################################################

    for tech, row in nrcan_techs.iterrows():

        tech_desc = f"{row.loc['end_use']} - {row.loc['description']}"
        curs.execute(f"""REPLACE INTO
                        technologies(tech, flag, sector, tech_desc, reference)
                        VALUES('{tech}', 'p', 'residential', '{tech_desc}', '{nrcan_ref}')""")

        # Get equivalent future tech
        aeo_class = row.loc['aeo_class']
        if pd.isna(aeo_class): continue

        # Lifetime from equivalent AEO tech
        equiv_tech = aeo_techs.loc[aeo_techs['aeo_class']==aeo_class].index.values[0]
        lifetime = config.lifetimes[equiv_tech]

        # Add lifetimes and feasible vintages to config dictionaries
        config.lifetimes[tech] = lifetime
        exs_vints = utils.stock_vintages(base_year, lifetime)
        config.tech_vints[tech] = exs_vints



    conn.commit()
    conn.close()
        


# For region-specific aggregation
def aggregate_region(region):

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    """
    ##############################################################
        Efficiency, CostInvest, CostFixed
    ##############################################################
    """

    cens_div = config.regions.loc[region, 'us_census_div']

    curr = config.params['aeo_currency']
    curr_year = config.params['aeo_currency_year']

    # Narrow down the dataframe with each nested section to speed things up
    df0 = aeo_res_equip.loc[(aeo_res_equip['Census Division'] == cens_div) | (aeo_res_equip['Census Division'] == 11)]


    ##############################################################
    # AEO data (new)
    ##############################################################

    # All technologies from aeo technologies input csv
    for tech, row in aeo_techs.iterrows():

        aeo_class = row['aeo_class']
        aeo_equip = row['aeo_equip']

        in_comm = fuel_commodities.loc[row['fuel'], 'comm']
        lifetime = config.lifetimes[tech]

        ## LifetimeTech
        note = '(y) Average of Weibull distribution.'
        curs.execute(f"""REPLACE INTO
                    LifetimeTech(regions, tech, life, life_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', '{tech}', {lifetime}, '{note}',
                    '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")

        if type(df0) is pd.DataFrame: df1 = df0.loc[aeo_equip]
        elif type(df0) is pd.Series: df1 = df0 # only one row remaining

        end_uses = row.loc['end_uses'].split("+")
        end_use_ids = row.loc['end_use_ids'].split("+")

        # All future periods are valid vintages
        for vint in config.tech_vints[tech]:
            
            if type(df1) is pd.DataFrame:
                df2 = df1.loc[(df1['First Year']<=vint) & (vint<=df1['Last Year'])]
                cost_invest = df2.loc[df2['New Construction Cost'] != 0]['New Construction Cost'].values[0]
            elif type(df1) is pd.Series:
                df2 = df1 # only one row remaining
                cost_invest = df2['New Construction Cost']

            ## CostInvest
            curs.execute(f"""REPLACE INTO
                        CostInvest(regions, tech, vintage, data_cost_invest, data_cost_year, data_curr,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{tech}', {vint}, {cost_invest}, {curr_year}, '{curr}',
                        '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")
            
            ## CostFixed
            cost_fixed = row['cost_fixed']
            if cost_fixed != 0:
                for period in config.model_periods:

                    if period < vint or vint + lifetime <= period: continue

                    curs.execute(f"""REPLACE INTO
                                CostFixed(regions, periods, tech, vintage, data_cost_fixed, data_cost_year, data_curr,
                                reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                                VALUES('{region}', {period}, '{tech}', {vint}, {cost_fixed}, {curr_year}, '{curr}',
                                '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")


            # For each end use the technology supplies (heat pumps do heating and cooling)
            for e in range(len(end_uses)):

                end_use = end_uses[e]
                end_use_id = int(end_use_ids[e])

                out_comm = end_use_demands.loc[end_use, 'comm']
                
                if type(df2) is pd.DataFrame: eff = df2.loc[(df2['End Use'] == int(end_use_id))]['Efficiency'].values[0]
                elif type(df2) is pd.Series: eff = df2['Efficiency'] # only one row remaining

                eff_metric = aeo_res_class.loc[aeo_res_class['End Use'] == int(end_use_id)].loc[aeo_class, 'Efficiency Metric']
                if type(eff_metric) is pd.Series: eff_metric = eff_metric.values[0]
                if eff_metric in config.params['conversion_factors']['efficiency'].keys(): eff *= config.params['conversion_factors']['efficiency'][eff_metric]

                ## Default Efficiency
                curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{region}', '{in_comm}', '{tech}', {vint}, '{out_comm}', {eff}, '(PJ/PJ) from {eff_metric}',
                        '{aeo_ref}', {aeo_year}, 1, 1, 1, 1, 3, 1)""")
    

    ##############################################################
    # NRCan data (existing)
    ##############################################################

    for tech, row in nrcan_techs.iterrows():
        
        ## CostFixed from aeo equivalent tech
        aeo_class = row['aeo_class']
        if pd.isna(aeo_class): continue

        equiv_tech = aeo_techs.loc[aeo_techs['aeo_class']==aeo_class].index.values[0]
        cost_fixed = aeo_techs.loc[equiv_tech, 'cost_fixed']

        note = f"Assumed same as {equiv_tech}."

        # Doing this by region so that some regions can be skipped at aggregation phase
        lifetime = config.lifetimes[tech]

        curs.execute(f"""REPLACE INTO
                    LifetimeTech(regions, tech, life, life_notes,
                    reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                    VALUES('{region}', '{tech}', {lifetime}, '{note}',
                    '{aeo_ref}', {aeo_year}, 2, 1, 1, 1, 1, 2)""")
        
        if cost_fixed == 0: continue
        for vint in config.tech_vints[tech]:
            for period in config.model_periods:
                
                if period < vint or vint + lifetime <= period: continue

                curs.execute(f"""REPLACE INTO
                            CostFixed(regions, periods, tech, vintage, cost_fixed_notes, data_cost_fixed, data_cost_year, data_curr,
                            reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                            VALUES('{region}', {period}, '{tech}', {vint}, '{note}', {aeo_techs.loc[equiv_tech, 'cost_fixed']}, {curr_year}, '{curr}',
                            '{aeo_ref}', {aeo_year}, 2, 1, 1, 1, 3, 3)""")
    
    """
    ##############################################################
        Capacity to Activity
    ##############################################################
    """

    ## Adding arbitrary c2a as a default value to all technologies
    note = ("Arbitrary but sufficiently high to satisfy demand in all hours. Actual activity cont"
            "rolled by AnnualCapacityFactor tables and DemandActivity constraint. Result is that all technologi"
            "es are utilised in consistent proportions throughout the year, according to relative size of annua"
            "l capacity factors.")

    ## NRCan existing stock
    for tech, row in nrcan_techs.iterrows():
        end_use = row['end_use']

        c2a = end_use_demands.loc[end_use, 'c2a']
        unit = f"{end_use_demands.loc[end_use, 'dem_unit']}/{end_use_demands.loc[end_use, 'cap_unit']}.y" # ACT/CAP.y
        curs.execute(f"""REPLACE INTO
                        CapacityToActivity(regions, tech, c2a, c2a_notes, dq_est)
                        VALUES('{region}', '{tech}', {c2a}, '({unit}) {note}', 0)""")

    ## AEO future stock
    for tech, row in aeo_techs.iterrows():
        end_uses = row['end_uses'].split('+')

        c2a = end_use_demands.loc[end_uses[0], 'c2a'] # Must be the same for all end uses anyway
        unit = f"{end_use_demands.loc[end_uses[0], 'dem_unit']}/{end_use_demands.loc[end_uses[0], 'cap_unit']}.y" # ACT/CAP.y
        curs.execute(f"""REPLACE INTO
                        CapacityToActivity(regions, tech, c2a, c2a_notes, dq_est)
                        VALUES('{region}', '{tech}', {c2a}, '({unit}) {note}', 0)""")
    

    conn.commit()
    conn.close()



def weather_map_data(region, us_data: list) -> pd.Series:

    us_state = config.regions.loc[region, 'us_state']
    us_station = str(config.regions.loc[region, 'us_station'])
    ca_station = str(config.regions.loc[region, 'ca_station'])
    map_file = f"weather_map_{us_state}_{us_station}-{region}_{ca_station}.csv"
    
    # If the mapper already exists then just use it
    if region in weather_maps.keys():return apply_weather_map(region, us_data) # already retrieved
    elif not config.params['force_generate_weather_maps'] and os.path.isfile(config.cache_dir + map_file): # taking from local cache
        print(f"Loading weather map {map_file} from local cache...")
        weather_maps[region] = np.loadtxt(config.cache_dir + map_file, dtype=float, delimiter=',')
        try:
            return apply_weather_map(region, us_data)
        except Exception as e: # if failed regenerate the map
            print(f"Failed to apply weather map from local cache. Regenerating. Error:\n{e}")
        
    ## Otherwise generate the map
    print(f"\nGenerating a weather-based data map from {us_state} to {region}...")

    # Days in each month of the year. Need because of dodgy index
    days_in_months = [31,28,31,30,31,30,31,31,30,31,30,31]

    # Weather data from the US
    df_us_wth = utils.get_data(
        config.params['weather']['us']['url']
        .replace('<st>', us_station)
        .replace('<y>', str(config.params['weather']['us']['year']))
        , index_col=1, usecols=range(15)
        )
    df_us_wth = df_us_wth.loc[df_us_wth.index.str.contains('53')] # TODO This may not apply to all stations watch out
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
    df_ca = utils.get_data(
        config.params['weather']['canada']['url']
        .replace('<st>', ca_station)
        .replace('<r>', region)
        .replace('<y>', str(base_year))
        , encoding='unicode_escape', usecols=range(12)
        ).iloc[0:8760]

    # A 2D matrix map of which US data points to use per Canadian datum
    weather_maps[region] = np.zeros((8760,8760))

    unmatched = 0.0
    for i, row in df_ca.iterrows():

        # For this datum, boolean vector of relevant data in US data vector
        row_map = 1.0*np.array((row['Temp (°C)'] < df_us['TMP']+1) &
            (row['Temp (°C)'] > df_us['TMP']-1) &
            (row['Dew Point Temp (°C)'] > df_us['DEW']-1) &
            (row['Dew Point Temp (°C)'] < df_us['DEW']+1)).transpose()
        
        # If no match, maybe Canadian temps are hotter or !colder! than all us temps
        if np.sum(row_map) == 0: # Did not find a match
            unmatched += 1

            # Hotter than anything in US record so take highest US temp day
            if row['Temp (°C)'] > np.max(df_us['TMP']) and row['Dew Point Temp (°C)'] > np.max(df_us['DEW']):
                row_map = 1.0*np.array(df_us['TMP'] == np.max(df_us['TMP'])).transpose()

            # Colder than anything in US record so take lowest US temp day 
            elif row['Temp (°C)'] < np.min(df_us['TMP']) and row['Dew Point Temp (°C)'] < np.min(df_us['DEW']):
                row_map = 1.0*np.array(df_us['TMP'] == np.min(df_us['TMP'])).transpose()
        
        # Get the mean of relevant US data and or set NaN if no mappable hours -> will be interpolated
        row_map *= np.nan if np.sum(row_map) == 0 else 1 / np.sum(row_map)

        # Save these matrix maps per region to save having to repeat the above slow process
        weather_maps[region][i,:] = row_map.copy()

    print(f"{round((1-unmatched/8760)*100, 1)}% of hours found +-1C temperature match.")

    # Cache the generated weather map locally
    np.savetxt(config.cache_dir + map_file, weather_maps[region], delimiter=',')
    print(f"Weather map generated and cached as {map_file}")

    return apply_weather_map(region, us_data)


def apply_weather_map(region, us_data: list):

    print(f"Applying weather map for {region}...")

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
    time_of_week = [day_of_week[h//24] * hour_of_day[h%24] for h in range(24*7)] # option a. hour of day factor times day of week factor. Use this
    #time_of_week = [np.mean(us_data[h:24*7*52:24*7]/weekly_avg) for h in range(24*7)] # option b. hour of week factor

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



# Doing all regions at once because some regions might share equivalent states and this process is slow
def aggregate_dsd():

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    """
    ##############################################################
        Demand specific distribution
    ##############################################################
    """

    us_year = config.params['weather']['us']['year']
    reference = (f"{config.params['resstock']['reference']}; "
                 f"{config.params['weather']['us']['reference'].replace('<y>', str(us_year))}; "
                 f"{config.params['weather']['canada']['reference'].replace('<y>', str(base_year))}")

    res_config = pd.read_csv(config.input_files + 'resstock.csv', index_col=0)
    cons = dict() # 8760 hourly energy consumption by state, housing type, and end use, (kWh)

    ## Get end use energy consumptions from resstock columns and divide by number of housing units represented
    for state in config.regions.loc[config.regions['include']]['us_state'].unique():

        cons[state] = dict()

        for housing_type, file_name in config.params['resstock']['housing_files'].items():
            cons[state][housing_type] = dict()

            df_res = utils.get_data(config.params['resstock']['url'].replace("<s>",state.upper()).replace("<f>", file_name).replace("<s>", state.lower()))
            stock = df_res['units_represented'].values[0]

            for end_use in config.end_use_demands.index:
                
                res_cols = res_config.loc[res_config['end_use'] == end_use]

                for res_col in res_cols.index:
                    
                    # Divide consumption by number of units represented to get consumption per household
                    con = df_res[res_col].iloc[[8760, *range(3, 4*8760-1, 4)]].clip(lower=0) / stock # 15-minutely so take every 4th

                    if end_use in cons[state][housing_type].keys(): cons[state][housing_type][end_use] += con
                    else: cons[state][housing_type][end_use] = con

    
    ## Multiply energy consumptions from resstock by province housing stocks, apply weather mapping, then normalise to DSD
    for region in config.model_regions:

        row = config.regions.loc[region]
        state = row['us_state']

        note = (f"ResStock data for {state} (NREL, 2021) aggregated by end use and mapped to {us_year} air temperature "
                f"and dew point temperature, taking the mean, from station {config.regions.loc[region, 'us_station']} (NCEI, {us_year}). "
                f"Remapped to {base_year} {region} weather from station {config.regions.loc[region, 'ca_station']}, "
                f"matching temperatures +-1°C and applying a diurnal adjustment as hour-of-day average divided by annual average. "
                f"Chronological linear interpolation for any missing data.")

        # Table 14: Total Households by Building Type and Energy Source
        t14 = utils.get_compr_db(region, 14, 9, 12)[base_year] / 100 # % shares

        # Create figure and axes
        fig, axs = pp.subplots(4, 3, figsize=(15, 10))  # 4 rows, 3 columns
        axs[-1, -1].axis('off')
        fig.suptitle(f"{region} demand specific distributions (blue). Weekly profile in red.")

        p = 0 # plot tracker
        for end_use, row in config.end_use_demands.iterrows():

            demand_comm = row['comm']

            # Consumption for each housing type times provincial stock of that housing type
            con = sum([t14[housing_type] * cons[state][housing_type][end_use] for housing_type in t14.index])

            # Map space heating, cooling to temperature and dew point temp (humidity). Note: this might introduce weather efficiency to the demand!
            time_of_week = None
            if row['use_weather_map']: con, time_of_week = weather_map_data(region, con.to_numpy())

            # Apply tolerance and normalise
            con.loc[con < con.mean() * config.params['dsd_tolerance']] = 0
            dsd = (con / con.sum()).to_list()

            # For plotting DSDs
            row = p // 3  # Integer division to determine row
            col = p % 3   # Modulo to determine column
            axs[row, col].plot(dsd)
            if time_of_week is not None: axs[row, col].twinx().plot(range(0,8736,52), time_of_week, 'r-') # time of week multipliers overlaid
            axs[row, col].set_title(end_use)
            p+=1

            try:
                for h in range(8760):
                    curs.execute(f"""REPLACE INTO
                                DemandSpecificDistribution(regions, season_name, time_of_day_name, demand_name, dsd, dsd_notes,
                                reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                                VALUES('{region}', '{config.time.loc[h, 'season']}', '{config.time.loc[h, 'time_of_day']}', '{demand_comm}', '{dsd[h]}', '{note}',
                                '{reference}', {us_year}, 3, 2, 1, {utils.dq_time(base_year, us_year)}, 3, 3)""")
            except: pp.show()

        pp.tight_layout()


    conn.commit()
    conn.close()



def aggregate_emissions():

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db
    

    """
    ##############################################################
        Emission Activity
    ##############################################################
    """

    emis_comm = config.params['emission_commodity']
    emis_units = config.params['emission_activity_units']

    # Get emissions factors for fuels in ktCO2eq/PJ_in
    emis_fact = pd.read_excel(config.input_files+"/ghg-emission-factors-hub.xlsx", skiprows=13, nrows=76, index_col=2)[['CO2 Factor', 'CH4 Factor', 'N2O Factor']].iloc[1::].dropna()
    emis_fact = emis_fact[pd.to_numeric(emis_fact['CO2 Factor'], errors='coerce').notnull()]
    for fact in emis_fact.columns: emis_fact[fact] *= conversion_factors['epa_units'][fact.strip(' Factor')] * conversion_factors['gwp'][fact.strip(' Factor')]
    emis_fact[emis_comm] = emis_fact.sum(axis=1)

    for tech in config.all_techs:

        # Valid vintages and efficiencies from Efficiency table
        rows = curs.execute(f"SELECT regions, input_comm, tech, vintage, output_comm, efficiency FROM Efficiency WHERE tech == '{tech}'").fetchall()

        for row in rows:

            # Input fuel by epa naming convention
            epa_fuel = fuel_commodities.loc[fuel_commodities['comm'] == row[1], 'epa_fuel'].values[0]
            if pd.isna(epa_fuel): continue # doesn't need emissions

            # EmissionActivity is tied to OUTPUT energy so divide by efficiency
            emis_act = emis_fact.loc[epa_fuel, emis_comm] / row[5]

            # Note assumed fuel
            note = f"Emissions factor using {epa_fuel} (EPA, {config.params['epa_year']}) divided by efficiency as emissions are per output unit energy."

            curs.execute(f"""REPLACE INTO
                        EmissionActivity(regions, emis_comm, input_comm, tech, vintage, output_comm, emis_act, emis_act_units, emis_act_notes,
                        reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                        VALUES('{row[0]}', '{emis_comm}', '{row[1]}', '{row[2]}', {row[3]}, '{row[4]}', {emis_act}, '{emis_units}', '{note}',
                        '{config.params['epa_reference']}', {config.params['epa_year']}, 2, 1, 1, 1, 1, 3)""")
    

    conn.commit()
    conn.close()



# For non-regional post-subsector aggregation
def aggregate_post():

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db
    

    """
    ##############################################################
        Existing time periods
    ##############################################################
    """

    # Add all existing vintages to existing time periods
    vints = set([fetch[0] for fetch in curs.execute(f"SELECT vintage FROM Efficiency").fetchall() if fetch[0] not in config.model_periods])

    for vint in vints:
        curs.execute(f"""INSERT OR IGNORE INTO
                        time_periods(t_periods, flag)
                        VALUES({vint}, 'e')""")


    conn.commit()
    conn.close()



# For regional post-subsector aggregation
def aggregate_region_post(region):

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db

    """
    ##############################################################
        Annual Capacity Factor
    ##############################################################
    """

    reference = f"{nrcan_ref}; {statcan_ref}"
        
    ## AEO future stock
    # Copy from NRCan existing stock    
    for tech, row in aeo_techs.iterrows():
        if pd.isna(row['nrcan_equiv']):
            print(f"{tech} has no specific NRCan equivalent and so will have no annual capacity factor.")
            continue # no NRCan equivalent given
        
        end_uses = row['end_uses'].split('+')
        nrcan_equivs = row['nrcan_equiv'].split('+')
        nrcan_equivs = [nrcan_techs.loc[nrcan_techs['end_use'] + " - " + nrcan_techs['description'] == nrcan_equiv].index.values[0] for nrcan_equiv in nrcan_equivs]

        for e in range(len(end_uses)):
            
            end_use = end_uses[e]
            nrcan_tech = nrcan_equivs[e]

            out_comm = end_use_demands.loc[end_use, 'comm']
            
            note = f"Assumed same as {nrcan_equivs[e]}"

            # Get annual capacity factor from equivalent nrcan tech for which we have data
            acf = curs.execute(f"SELECT max_acf FROM MaxAnnualCapacityFactor WHERE tech == '{nrcan_tech}' and regions == '{region}'").fetchone()[0]

            for period in config.model_periods:
                curs.execute(f"""REPLACE INTO
                                MinAnnualCapacityFactor(regions, periods, tech, output_comm, min_acf, min_acf_notes,
                                reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                                VALUES('{region}', {period}, '{tech}', '{out_comm}', {acf*0.99}, '{note}',
                                '{reference}', {base_year}, 2, 1, 1, {utils.dq_time(period, base_year)}, 3, 3)""")
                curs.execute(f"""REPLACE INTO
                                MaxAnnualCapacityFactor(regions, periods, tech, output_comm, max_acf, max_acf_notes,
                                reference, data_year, dq_est, dq_rel, dq_comp, dq_time, dq_geog, dq_tech)
                                VALUES('{region}', {period}, '{tech}', '{out_comm}', {acf}, '{note}',
                                '{reference}', {base_year}, 2, 1, 1, {utils.dq_time(period, base_year)}, 3, 3)""")
         


    conn.commit()
    conn.close()



def cleanup():

    # Connect to the new database file
    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor() # Cursor object interacts with the sqlite db



    """
    ##############################################################
        Existing tech with no capacity
    ##############################################################
    """

    # Get all tables with tech and region indices
    all_tables = [fetch[0] for fetch in curs.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]
    t_tables = [table for table in all_tables if 'tech' in [description[0] for description in curs.execute(f"SELECT * FROM '{table}'").description]]
    tr_tables = [table for table in t_tables if 'regions' in [description[0] for description in curs.execute(f"SELECT * FROM '{table}'").description]]

    for region in config.model_regions:
        for tech, row in config.nrcan_techs.iterrows():
            if row['end_use'] == 'appliances other': continue # Does not have capacity

            exs_cap = sum([fetch[0] for fetch in curs.execute(f"SELECT exist_cap FROM ExistingCapacity WHERE tech == '{tech}' and regions == '{region}'").fetchall()])
            if exs_cap == 0:
                
                # If no existing capacity for an existing tech, purge tech/region combo from database
                for table in tr_tables: 
                    curs.execute(f"DELETE FROM '{table}' WHERE tech == '{tech}' AND regions == '{region}'")

                print(f"Cleaned up existing tech with no existing capacity: {region}, {tech}")

    

    """
    ##############################################################
        End uses with no (one) choice
    ##############################################################
    """

    # For each region
        # For each end use
            # Check end use technologies
            # If only one that isn't -EXS then no choice
            # Determine average efficiency by period from ACFs, existing capacities/lifetimes, and efficiencies
            # slate end use technologies for removal
            # Add single new dummy technology with average efficiency by period (new vintage for each period?)
            # D_R_FRZ -> R_FRZ


    conn.commit()
    conn.close()