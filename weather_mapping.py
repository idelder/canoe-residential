import pandas as pd
import numpy as np
import utils
import os
from datetime import datetime
from setup import config

weather_maps = dict() # Maps that have already been loaded weather_maps[region] = 8760x8760 np array

def map_data(region: str, us_data: np.ndarray) -> tuple[pd.Series, np.ndarray]:

    us_state = config.regions.loc[region, 'us_state']
    us_station = str(config.regions.loc[region, 'us_station'])
    ca_station = str(config.regions.loc[region, 'ca_station'])
    map_file = f"weather_map_{us_state}_{us_station}-{region}_{ca_station}_{str(config.params['weather_year'])}.csv"
    
    # If the mapper already exists then just use it
    # Already loaded
    if region in weather_maps.keys(): return apply_map(region, us_data)
    # Load from local cache
    elif not config.params['force_generate_weather_maps'] and os.path.isfile(config.cache_dir + map_file):
        print(f"Loading weather map {map_file} from local cache...")
        weather_maps[region] = np.loadtxt(config.cache_dir + map_file, dtype=float, delimiter=',')
        try:
            return apply_map(region, us_data)
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
        , name=f"climate_us_{us_state}_{us_station}_{str(config.params['weather']['us']['year'])}.csv"
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
                idx = f"2018-{ms}-{ds}T{hs}:53:00" # TODO This may not apply to all stations watch out

                for val in ['TMP','DEW']:
                    if idx not in df_us_wth.index: v = pd.NA
                    else:
                        v = float(df_us_wth.loc[idx, val].split(',')[0])/10
                        if v > 50: v = pd.NA
                    df_us.loc[i, val] = v
                # Including wind speed to potentially improve modelling
                for val in ['WND']:
                    if idx not in df_us_wth.index: v = pd.NA
                    else:
                        v = float(df_us_wth.loc[idx, val].split(',')[-2])/10
                        if v > 500: v = pd.NA
                    df_us.loc[i, val] = v

                i+=1

    # Fill in data gaps by chronological linear interpolation
    for col in ['TMP','DEW','WND']: df_us[col].interpolate(method='linear', inplace=True)

    # Canadian weather data
    df_ca = utils.get_data(
        config.params['weather']['canada']['url']
        .replace('<st>', ca_station)
        .replace('<r>', region)
        .replace('<y>', str(config.params['weather_year']))
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
        
        # If no match, maybe Canadian temps are hotter or (more likely) colder than all us temps
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

    return apply_map(region, us_data)



def apply_map(region: str, us_data: np.ndarray) -> tuple[pd.Series, np.ndarray]:

    print(f"Applying weather map for {region}...")

    # Canadian data starts as us data mapped to temperature and dew point temperature
    ca_data = pd.Series(np.matmul(weather_maps[region], us_data)).interpolate(method='linear')

    # Then get the day of week of Jan 1 for each year. Monday is 0, Sunday 6
    jan_1_us = datetime.weekday(datetime.fromisoformat(f"{config.params['weather']['us']['year']}-01-01"))
    jan_1_ca = datetime.weekday(datetime.fromisoformat(f"{config.params['weather_year']}-01-01"))

    # Get multipliers for time of the week, hourly -> this doesnt work as temperature effects are double counted
    daily_avg = np.array([np.mean(us_data[24*d:24*d+23]) for d in range(364)])
    weekly_avg = np.array([np.mean(us_data[7*24*w:7*24*w+7*24-1]) for w in range(52)])
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
    #ca_data *= tow_mults
    
    # Return mapped data and time-of-week multipliers Mon -> Sun
    return ca_data, time_of_week_zeroed/np.mean(time_of_week_zeroed)