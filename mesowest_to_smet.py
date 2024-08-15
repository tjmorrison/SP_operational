#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#%%
"""
Created on Sat Jul 20 09:39:04 2024



@author: Travis Morrison
Add send email at end of running with printout
"""
import sys
import requests
import json
import numpy as np
from datetime import datetime, timedelta, timezone
import pandas as pd
#!!!
#import hrrr



def mesowest_to_smet(start_time, current_time,stid,make_input_plot,forecast_bool):
    """
    Function to collect data from the mesowest api and create a snowpack input file
    
    TODO: Add forecast padding option 
          Streamline function with snowpat python library from the SLF
          

    Parameters
    ----------
    start_time : string
        start time for data in YYYYMMDDHHmmss. Time in UTC
    end_time : string
        end time for data in YYYYMMDDHHmmss. Time in UTC.
    stid : string
        station ID from Mesowest. Atwater is ATH20.
    make_input_plot : boolean
        DESCRIPTION.
    forecast bool : boolean
        DESCRIPTION.

    Returns
    -------
    None.

    """
    print("Building *.smet file for " + stid + " from " + start_time + " to " + current_time)
    
    # build API url
    mesowest_url = f'http://api.mesowest.net/v2/stations/timeseries?stid={stid}&token=3d5845d69f0e47aca3f810de0bb6fd3f&start={start_time}&end={current_time}'
    #print(mesowest_url)
    
    # Call mesowest API
    response = requests.get(mesowest_url)
    data = response.json()
    observations = data['STATION'][0]['OBSERVATIONS']
    data_length = len(observations['date_time'])
    
    #allocate the date time arrays
    years = []
    months = []
    days = []
    hours = []
    minutes = []
    seconds = []
    
    # Collect the datetime vars from the mesowest station
    years = [int(dt[0:4]) for dt in observations['date_time']]
    months = [int(dt[5:7]) for dt in observations['date_time']]
    days = [int(dt[8:10]) for dt in observations['date_time']]
    hours = [int(dt[11:13]) for dt in observations['date_time']]
    minutes = [int(dt[14:16]) for dt in observations['date_time']]
    seconds = [int(dt[17:19]) for dt in observations['date_time']]

    # Print out current time and station last obs time to user
    station_last_obs_time = str(years[-1])+'-'+str(months[-1]).zfill(2)+'-'+str(days[-1]).zfill(2)+'T'+str(hours[-1]).zfill(2)+':'+str(minutes[-1]).zfill(2)+':00'
   
    
    #end_date = datetime.strptime(station_last_obs_time, '%y%m%d%H%M%S')
    print(datetime.strptime(station_last_obs_time, '%Y-%m-%dT%H:%M:%S'))

    print("Station last obs time is: " + station_last_obs_time)
    print("Current time is: " + current_time)
    print("Simulation will run to: " + station_last_obs_time)

    # Write end date to a file for use in SNOWPACK workflow - Note that this handles errors associated 
    # with the current time not matching the last obs from the Wx station
    filename = 'smet_end_datetime.dat'
    with open(filename, 'w') as file:
        file.write(f'end_year = {years[-1]}\n')
        file.write(f'end_month = {str(months[-1]).zfill(2)}\n')
        #file.write(f'end_mon_10 = {end_mon_10}\n')
        file.write(f'end_day = {str(days[-1]).zfill(2)}\n')
        #file.write(f'end_10 = {end_10}\n')
        file.write(f'end_hour = {str(hours[-1]).zfill(2)}\n')
        file.write(f'end_min = {str(minutes[-1]).zfill(2)}\n')

    print(f"Station end time written to {filename}")
    
    # Add specific vars adjustment - all stations
    TA = [temp + 273.15 for temp in observations['air_temp_set_1']]
    TSS = [temp + 273.15 for temp in observations['surface_temp_set_1']]
    RH = [rh / 100.0 for rh in observations['relative_humidity_set_1']]
    ISWR = observations['solar_radiation_set_1']
    #Removing negative solar radation values
    for i in range(len(ISWR)):
        if ISWR[i]<0:
            ISWR[i] = 0
    VW = observations['wind_speed_set_1']
    DW = observations['wind_direction_set_1']
    HS = [depth / 1000.0 for depth in observations['snow_depth_set_1']]
    TSG = [273.15] * len(HS)  # Ground surface temperature assumed to be 0°C
    #PSUM = 

    # Apply specific station adjustments
    # HS 
    HS[:450] = np.zeros(450)
    HS[4196] = HS[4195]
    HS[4323] = HS[4322]
    HS[4197:4262] = [depth - 2 for depth in HS[4197:4262]]
 
    # Replace NaNs with -999
    TA = [-999 if val is None else val for val in TA]
    TSS = [-999 if val is None else val for val in TSS]
    RH = [-999 if val is None else val for val in RH]
    ISWR = [-999 if val is None else val for val in ISWR]
    VW = [-999 if val is None else val for val in VW]
    DW = [-999 if val is None else val for val in DW]
    HS = [-999 if val is None else val for val in HS]
    TSG = [-999 if val is None else val for val in TSG]

    # Print data out to SMET file
    StationID = data['STATION'][0]['STID']
    StationName = data['STATION'][0]['NAME']
    latitude = float(data['STATION'][0]['LATITUDE'])
    longitude = float(data['STATION'][0]['LONGITUDE'])
    altitude = float(data['STATION'][0]['ELEV_DEM']) * 0.3048
    UTM_zone = None  # Calculate UTM zone if needed
    source = 'University of Utah EFD Lab'
    
    fileID = open(f'{StationID}.smet', 'w')
    
    fileID.write('SMET 1.1 ASCII\n')
    fileID.write('[HEADER]\n')
    fileID.write(f'station_id       = {StationID}\n')
    fileID.write(f'station_name     = {StationName}\n')
    fileID.write(f'latitude         = {latitude}\n')
    fileID.write(f'longitude        = {longitude}\n')
    fileID.write(f'altitude         = {altitude}\n')
    #fileID.write(f'easting          = {easting}\n')
    #fileID.write(f'northing         = {northing}\n')
    fileID.write('nodata           = -999\n')
    fileID.write('tz               = 1\n')
    fileID.write(f'source           = {source}\n')
    fileID.write(f'fields           = timestamp TA RH TSG TSS HS VW DW ISWR\n')
    fileID.write('[DATA]\n')


    date = []
    for i in range(len(HS)):
        date.append(datetime(years[i], months[i], days[i], hours[i], minutes[i], seconds[i]))
        iso_date = date[i].isoformat()
        fileID.write(f'{iso_date} {TA[i]:.2f} {RH[i]:.2f} {TSG[i]:.2f} {TSS[i]:.2f} {HS[i]:.2f} {VW[i]:.2f} {DW[i]:.2f} {ISWR[i]:.2f}\n')
    
    fileID.close()

    if forecast_bool == True:
        # Maximum number of parallel processes if being run parallel
        maxprocesses = 10

        # Site coordinates (currently Atwater based on google maps)
        #!!! Need to double check, ... but should use vars above
        sitelat = 40.591230
        sitelon = -111.637711  #actual Atwater, yields HRRR grid point to east (2928 m elevation)
        #sitelon = -111.660  # slightly down canyon, yields HRRR grid point to west (2825 m elevation)

        #forecast_start_time = end_time#datetime.now(timezone.utc) - timedelta(hours=1) 

        #forecast_df = hrrr.get_hrrr_forecast(forecast_start_time,sitelat,sitelon,siteelev = altitude,mlthick = 300,maxprocesses = maxprocesses)
        #load csv for debugging
        forecast_df =  pd.read_csv('/Users/travismorrison/Documents/GitHub/UAC-Snowpack/hrrr-snowpack/hrrr_to_snowpack_2024031818.csv',header=0)
        #Need to append forecast data to file....
                
        # Columns to be selected
        columns_to_write = ['INIT (YYYYMMDDHH UTC)','T2m (K)', 'RH2m (%)','TSFC (K)','Snowfall (cm)','Wind Speed 10m (m/s)','Wind Direction 10 m (deg)','Downward Short Wave (W/m2)'] # Is TSFC surface temp, or need to derive from LW??


        #Need to correct date and add TSG (= 273.15)
        forecast_df['INIT (YYYYMMDDHH UTC)'] =  [datetime.strptime(station_last_obs_time, '%Y-%m-%dT%H:%M:%S') + timedelta(hours=1) for x in range(48)] 

        forecast_df['RH2m (%)'] = forecast_df['RH2m (%)']/100

        # Selecting the specific columns
        df_selected = forecast_df[columns_to_write][1:].round(2)

        # Write the selected columns to the file, appending it 
        df_selected.to_csv(f'{StationID}.smet', mode='a', header=False, index=False, sep='\t')

    # Make time series plot of the input data
    if make_input_plot == True:
        import matplotlib.pyplot as plt
        from matplotlib.dates import DateFormatter, AutoDateLocator
        
        # Make plots
        plt.figure(figsize=(10, 12))
        
        # Plot 1
        plt.subplot(5, 1, 1)
        plt.plot(date, VW)
        plt.gca().xaxis.set_major_formatter(DateFormatter('%m/%d'))
        plt.xlabel('UTC')
        plt.ylabel('Wind Speed (m/s)')
        plt.title('Atwater Peak SNOWPACK data')
        plt.gca().xaxis.set_major_locator(AutoDateLocator())
        plt.gcf().autofmt_xdate()
        plt.grid(True)
    
    
        # Plot 2
        plt.subplot(5, 1, 2)
        plt.plot(date, ISWR)
        plt.gca().xaxis.set_major_formatter(DateFormatter('%m/%d'))
        plt.xlabel('UTC')
        plt.ylabel('Solar Radiation (W/m^2)')
        plt.gca().xaxis.set_major_locator(AutoDateLocator())
        plt.gcf().autofmt_xdate()
        plt.grid(True)
        
        # Plot 3
        plt.subplot(5, 1, 3)
        plt.plot(date, TSS, label='Surface Temp')
        plt.plot(date, TA, label='Air Temp')
        plt.gca().xaxis.set_major_formatter(DateFormatter('%m/%d'))
        plt.xlabel('UTC')
        plt.ylabel('Temperatures (C)')
        plt.legend()
        plt.gca().xaxis.set_major_locator(AutoDateLocator())
        plt.gcf().autofmt_xdate()
        plt.grid(True)
        
        
        # Plot 4
        plt.subplot(5, 1, 4)
        plt.plot(date, [rh * 100 for rh in RH])
        plt.gca().xaxis.set_major_formatter(DateFormatter('%m/%d'))
        plt.xlabel('UTC')
        plt.ylabel('RH (%)')
        plt.gca().xaxis.set_major_locator(AutoDateLocator())
        plt.gcf().autofmt_xdate()
        plt.grid(True)
        
    
        # Plot 5
        plt.subplot(5, 1, 5)
        plt.plot(date, HS)
        plt.gca().xaxis.set_major_formatter(DateFormatter('%m/%d'))
        plt.xlabel('UTC')
        plt.ylabel('Snow Depth (cm)')
        plt.gca().xaxis.set_major_locator(AutoDateLocator())
        plt.gcf().autofmt_xdate()
        plt.grid(True)
        
        plt.tight_layout()
        plt.savefig('./figures/' + stid + ''+ start_time + '_' + end_time + '_timeseries.png')
        
def get_current_time(write_current_time = True):
    """
    Function to get current time and print time to file for bash script to call snowpack
    
    TODO: 

    Parameters
    ----------
    None.

    Returns
    -------
    current_time : string
        current datetime for data in YYYYMMDDHHmmss. Time in UTC

    """
        
    # Compute end time based on most recent 0000 UTC observation
    now = datetime.now(timezone.utc)  # Current UTC datetime
    current_year = now.strftime('%Y')
    current_month = now.strftime('%m')
    current_mon_10 = (now + timedelta(days=14)).strftime('%m')
    current_day = now.strftime('%d')
    current_10 = (now + timedelta(days=14)).strftime('%d')
    current_hour = now.strftime('%H')
    current_min = '00'  # For now, all simulations end at 0000 UTC

    # Write end date to a file for use in SNOWPACK workflow
    
    if write_current_time == True:
        filename = 'smet_current_datetime.dat'
        with open(filename, 'w') as file:
            file.write(f'current_year = {current_year}\n')
            file.write(f'current_month = {current_month}\n')
            file.write(f'current_mon_10 = {current_mon_10}\n')
            file.write(f'current_day = {current_day}\n')
            file.write(f'current_10 = {current_10}\n')
            file.write(f'current_hour = {current_hour}\n')
            file.write(f'current_min = {current_min}\n')

        print(f"Current time written to {filename}")

    current_time = str(current_year+current_month+current_day+current_hour+current_min) # YYYYMMDDHHMM UTC
    #print('Current time is:' + current_time)
    return current_time, current_year, current_month

if __name__ == "__main__":
    
    # Set default arguments
    current_time, current_year, current_month = get_current_time() # YYYYMMDDHHMM UTC
    if (int(current_month) < 10):
        start_time = str(int(current_year) - 1) + '10050000' # YYYYMMDDHHMM UTC, always 1 year behind on oct 5 
    else:
        start_time = current_year + '10050000' # YYYYMMDDHHMM UTC 
    make_input_plot = False
    stid = 'ATH20' #Defualt is atwater study plot
    forecast_bool = True

    
    # Set default values or use command-line arguments
    var1 = sys.argv[1] if len(sys.argv) > 1 else start_time
    var2 = sys.argv[2] if len(sys.argv) > 2 else current_time
    var3 = sys.argv[3] if len(sys.argv) > 3 else stid
    var4 = sys.argv[4] if len(sys.argv) > 4 else make_input_plot
    var5 = sys.argv[5] if len(sys.argv) > 5 else forecast_bool

    # Call mesowest to smet converter
    mesowest_to_smet(var1, var2, var3, var4, var5)
    
    
    
    
    
