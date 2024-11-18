#!/usr/bin/env python
# coding: utf-8
# Downloads HRRR and produces inputs for SNOWPACK model
#
# Will acquire real-time data if nothing is passed to script on command line
# Use YYYY-MM-DD HH:00:00 after script name to acquire specific model run
#
# May requires python 3.9 (or higher) and other dependencies
# Jim Steenburgh 18 March 2024
#
import numpy as np
import pandas as pd
import xarray as xr
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import datetime
import os
import requests
import sys
import glob
from metpy.units import units
from metpy.calc import wind_direction
from metpy.calc import wet_bulb_temperature
import cfgrib
from scipy import spatial
from sklearn.neighbors import KDTree
from multiprocessing import Pool
from platform import python_version
import warnings
warnings.filterwarnings('ignore')
import time




# Downloads HRRR from AWS, identifies or calculates needed variables, and finds values for closest grid point to site coordinates
# Use grib_ls <gribfilename> on the commandline on the linux system for complete list of shortName, typeOfLevel, etc.
def processhrrr (yr, mn, dy, hr, fhr, sitelat, sitelon,siteelev,mlthick, scratchdir):

    # File names and URLs on AWS and local disk
    serverfile = 'hrrr.t'+str(hr)+'z.wrfprsf'+str(fhr).zfill(2)+'.grib2'
    localfile = scratchdir+str(yr)+str(mn)+str(dy)+str(hr)+'F'+str(fhr).zfill(2)+'hrrr.grib2'
    awsbucket_name = 'noaa-hrrr-bdp-pds'
    awsobject_key = 'hrrr.'+str(yr)+str(mn)+str(dy)+'/conus/'+serverfile

    # boto3 settings
    s3 = boto3.client('s3',
                      config=Config(                                                                                                                            
                          signature_version=UNSIGNED,  # Specify your signature version if required                                                             
                          connect_timeout=5,           # Set connection timeout to 5 seconds                                                                             
                          read_timeout=30,             # Set read timeout to 30 seconds                                                                                 
                          retries={                                                                                                                             
                              'max_attempts': 3,       # Set the maximum number of retry attempts                                                                    
                              'mode': 'standard'       # Use the standard retry mode                                                                                
                          }                                                                                                                                     
                      ))


    # Try to retrieve from AWS
    awsraise = 0
    try:                                                                                                                                                        
        s3.head_object(Bucket=awsbucket_name, Key=awsobject_key)                                                                                                
    except:
        awsraise = 1

    if awsraise == 0:
        s3.download_file(awsbucket_name,awsobject_key,localfile)
    else:
        print(serverfile+' not available')
        raise

    # Load needed variables and rename to something less obtuse if needed
    # Surface elevation (m)
    orog_in = xr.open_dataset(localfile, decode_coords='all', engine='cfgrib', filter_by_keys={'stepType': 'instant',
                                        'typeOfLevel': 'surface', 'shortName':'orog'}) 

    # Grid-relative 10-m u wind (m/s)
    u10m_in = xr.open_dataset(localfile, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround',
                                        'stepType':'instant', 'shortName':'10u'}).rename({'u10':'u10m'})
    
    # Grid-relative 10-m v wind (m/s)
    v10m_in = xr.open_dataset(localfile, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround',
                                        'stepType':'instant', 'shortName':'10v'}).rename({'v10':'v10m'}) 
    
    # 2-m temperature (K)
    t2m_in = xr.open_dataset(localfile, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround',
                                        'stepType':'instant', 'shortName':'2t'})   # Variable name is t2m in dataset

    # 2-m rh (%)
    rh2m_in = xr.open_dataset(localfile, engine='cfgrib', filter_by_keys={'typeOfLevel': 'heightAboveGround',
                                        'stepType':'instant', 'shortName':'2r'}).rename({'r2':'rh2m'})
    
    # Surface temperature (K)
    tsfc_in = xr.open_dataset(localfile, engine='cfgrib', filter_by_keys={'typeOfLevel': 'surface',
                                        'stepType':'instant', 'shortName':'t'}).rename({'t':'tsfc'}) 
    
    # Surface pressure (Pa)
    psfc_in = xr.open_dataset(localfile, engine='cfgrib', filter_by_keys={'typeOfLevel': 'surface',
                                        'stepType':'instant', 'shortName':'sp'}).rename({'sp':'psfc'}) 
    
    # Surface downward shortwave radiation (W/m**2)
    dswrf_in = xr.open_dataset(localfile, engine='cfgrib', filter_by_keys={'typeOfLevel': 'surface',
                                        'stepType':'instant', 'shortName':'dswrf'})

    # Surface upward shortwave radiation (W/m**2)
    uswrf_in = xr.open_dataset(localfile, engine='cfgrib', filter_by_keys={'typeOfLevel': 'surface',
                                        'stepType':'instant', 'shortName':'uswrf'})

    # Surface downward longwave radiation (W/m**2)
    dlwrf_in = xr.open_dataset(localfile, engine='cfgrib', filter_by_keys={'typeOfLevel': 'surface',
                                        'stepType':'instant', 'shortName':'dlwrf'})
    
    # Surface upward longwave radiation (W/m**2)
    ulwrf_in = xr.open_dataset(localfile, engine='cfgrib', filter_by_keys={'typeOfLevel': 'surface',
                                        'stepType':'instant', 'shortName':'ulwrf'})
    
    # Grid-relative pressure-level u wind (m/s) 
    upress_in = xr.open_dataset(localfile, decode_coords='all', engine='cfgrib', filter_by_keys={'typeOfLevel': 'isobaricInhPa',
                                        'stepType':'instant', 'shortName':'u'}).rename({'u':'upress'}) 

    # Grid-relative pressure-level v wind (m/s)
    vpress_in = xr.open_dataset(localfile, decode_coords='all', engine='cfgrib', filter_by_keys={'typeOfLevel': 'isobaricInhPa',
                                        'stepType':'instant', 'shortName':'v'}).rename({'v':'vpress'}) 
    
    # Pressure-level geopotential height (m)
    hpress_in = xr.open_dataset(localfile, decode_coords='all', engine='cfgrib', filter_by_keys={'typeOfLevel': 'isobaricInhPa',
                                        'stepType':'instant', 'shortName':'gh'}).rename({'gh':'hpress'}) 

    # Pressure level temperature 
    tpress_in = xr.open_dataset(localfile, decode_coords='all', engine='cfgrib', filter_by_keys={'typeOfLevel': 'isobaricInhPa',
                                        'stepType':'instant', 'shortName':'t'}).rename({'t':'tpress'}) 

    # Pressure level dewpoint temperature
    tdpress_in = xr.open_dataset(localfile, decode_coords='all', engine='cfgrib', filter_by_keys={'typeOfLevel': 'isobaricInhPa',
                                        'stepType':'instant', 'shortName':'dpt'}).rename({'dpt':'tdpress'}) 

    # Hourly water-equivalent precipitation (kg/m**2 which is same as mm)
    if fhr == 0:
        hourly_precip_in = xr.open_dataset(localfile, engine='cfgrib', filter_by_keys={
                                        'stepRange': str(fhr), 'shortName':'tp'}).rename({'tp' : 'hourlyprecip'})
    else:
        prev_fhr = fhr - 1
        hourly_precip_in = xr.open_dataset(localfile, engine='cfgrib', filter_by_keys={
                                        'stepRange': str(prev_fhr)+'-'+str(fhr), 'shortName':'tp'}).rename({'tp' : 'hourlyprecip'})

    # Merge into single dataset
    hrrrdata = xr.merge([orog_in,u10m_in,v10m_in,t2m_in,rh2m_in, tsfc_in, psfc_in, dswrf_in,uswrf_in,dlwrf_in,
                         ulwrf_in,upress_in,vpress_in,hpress_in,tpress_in,tdpress_in,hourly_precip_in], compat='override') 

    # Calculate 10-m wind speed
    hrrrdata = hrrrdata.assign(wspd10m=(hrrrdata['u10m']**2 + hrrrdata['v10m']**2)**0.5)

    # Calculate pressure level wind speeds
    hrrrdata = hrrrdata.assign(wspdpress=(hrrrdata['upress']**2 + hrrrdata['vpress']**2)**0.5)
    
    # Grid rotate winds and store as u10mearth and v10mearth (see https://rapidrefresh.noaa.gov/faq/HRRR.faq.html)
    rotcon, lonp, latp =.0622515, -97.5, 38.5
    angle2 = rotcon*(hrrrdata['latitude'] - lonp)*0.017453
    sinx2, cosx2 = np.sin(angle2), np.cos(angle2)
    hrrrdata = hrrrdata.assign(u10m_er=cosx2*hrrrdata['u10m']+sinx2*hrrrdata['v10m'])
    hrrrdata = hrrrdata.assign(v10m_er=-sinx2*hrrrdata['u10m']+cosx2*hrrrdata['v10m'])
    hrrrdata.u10m_er.attrs['units'] = 'm s**-1'
    hrrrdata.v10m_er.attrs['units'] = 'm s**-1'

    # Calculate earth-relative wind direction
    hrrrdata = hrrrdata.assign(wdir10m=wind_direction(hrrrdata['u10m_er'],hrrrdata['v10m_er']))

    # Get data for gridpoint closest to site coordinates and save in dataframe
    # See https://github.com/blaylockbk/pyBKB_v3/blob/master/demo/KDTree_nearest_neighbor.ipynb
    lons = hrrrdata.longitude.values
    lats = hrrrdata.latitude.values
    tree = spatial.KDTree(np.column_stack([lons.ravel(), lats.ravel()]))
    point = np.array([sitelon+360., sitelat])  # Add 360 to match HRRR lons 
    dist, idx = tree.query(point)
    y,x = np.unravel_index(idx,lons.shape)
    yyyymmddhh = str(yr)+str(mn)+str(dy)+str(hr)
    ifhr = int(fhr)
    nearest_lat = np.round(hrrrdata.latitude.isel(x=x,y=y).values,3)   
    nearest_lon = np.round(hrrrdata.longitude.isel(x=x,y=y).values - 360.,3)  # Subtract 360 for negative wes lon
    nearest_elev = np.round(hrrrdata.orog.isel(x=x,y=y).values,3)
    nearest_psfc = np.round(hrrrdata.psfc.isel(x=x,y=y).values,3)
    nearest_tsfc = np.round(hrrrdata.tsfc.isel(x=x,y=y).values,3)
    nearest_t2m = np.round(hrrrdata.t2m.isel(x=x,y=y).values,3)
    nearest_rh2m = np.round(hrrrdata.rh2m.isel(x=x,y=y).values,3)
    nearest_wspd10m = np.round(hrrrdata.wspd10m.isel(x=x,y=y).values,3)
    nearest_wdir10m = np.round(hrrrdata.wdir10m.isel(x=x,y=y).values,3)
    nearest_dswrf = np.round(hrrrdata.dswrf.isel(x=x,y=y).values,3)
    nearest_uswrf = np.round(hrrrdata.uswrf.isel(x=x,y=y).values,3)
    nearest_dlwrf = np.round(hrrrdata.dlwrf.isel(x=x,y=y).values,3)
    nearest_ulwrf = np.round(hrrrdata.ulwrf.isel(x=x,y=y).values,3)
    nearest_hourlyprecip = np.round(hrrrdata.hourlyprecip.isel(x=x,y=y).values,3)

    # Create height-level data (AGL) needed for producing SLR forecast
    data = {
        'T05K' : [valheight(hrrrdata.tpress.isel(x=x,y=y).values,hrrrdata.hpress.isel(x=x,y=y).values,siteelev+500)],
        'T1K'  : [valheight(hrrrdata.tpress.isel(x=x,y=y).values,hrrrdata.hpress.isel(x=x,y=y).values,siteelev+1000)],  
        'T2K'  : [valheight(hrrrdata.tpress.isel(x=x,y=y).values,hrrrdata.hpress.isel(x=x,y=y).values,siteelev+2000)],  
        'SPD05K' : [valheight(hrrrdata.wspdpress.isel(x=x,y=y).values,hrrrdata.hpress.isel(x=x,y=y).values,siteelev+500)],
        'SPD1K'  : [valheight(hrrrdata.wspdpress.isel(x=x,y=y).values,hrrrdata.hpress.isel(x=x,y=y).values,siteelev+1000)],
        'SPD2K'  : [valheight(hrrrdata.wspdpress.isel(x=x,y=y).values,hrrrdata.hpress.isel(x=x,y=y).values,siteelev+2000)]
    }
    slrdf = pd.DataFrame(data)

    # Load keys, scalar, and model and run random forest for slr
    #keys = np.load('./SingleSite_slr_model_keysAGL30.npy', allow_pickle=True)
    #scaler = np.load('./SingleSite_slr_model_scalerAGL30.npy', allow_pickle=True)[()]
    #model = np.load('./SingleSite_RF_slr_modelAGL30.pickle', allow_pickle=True)
    #data_norm = pd.DataFrame(scaler.transform(slrdf), index=slrdf.index, columns=slrdf.keys())
    #nearest_slr = float(model.predict(data_norm))

    # Get wet bulb temperature profile for snow level calculation
    nearest_wbprofile = wet_bulb_temperature(hrrrdata.isobaricInhPa.values * units.hPa,
                                             hrrrdata.tpress.isel(x=x,y=y).values * units.degK,
                                             hrrrdata.tdpress.isel(x=x,y=y).values * units.degK)
    

    # Determine wet-bulb zero height 
    nearest_wbzheight = np.round(calcwbzlevel(nearest_wbprofile.magnitude - 273.15, hrrrdata.hpress.isel(x=x,y=y).values),1)
    
    
    # Adjust SLR if site below wet-bulb zero height
    nearest_slr = 0 # allocate slr otherwise throws error if not assigned in if statement
    # Decreases SLR linearly to zero at mlthick distance below wet-bulb zero height
    if nearest_wbzheight > siteelev and nearest_wbzheight < siteelev + mlthick:
        nearest_slr = nearest_slr*(siteelev+mlthick-nearest_wbzheight)/mlthick
    elif nearest_wbzheight > siteelev+mlthick:
        nearest_slr = 0.0

    # If SLR < 3 just make it zero
    if nearest_slr < 3:
        nearest_slr = 0

    nearest_slr = np.round(nearest_slr,1)
        
    # Calculate snowfall...convert to cm
    nearest_snow = np.round((nearest_hourlyprecip * nearest_slr)/10,1)
    
    return (
        yyyymmddhh, ifhr, nearest_lat, nearest_lon, nearest_elev, nearest_psfc, nearest_tsfc, nearest_t2m, nearest_rh2m,
        nearest_wspd10m, nearest_wdir10m, nearest_dswrf, nearest_uswrf, nearest_dlwrf,
        nearest_ulwrf, nearest_hourlyprecip, nearest_wbzheight, nearest_slr, nearest_snow
        )

# Calculates height of wet-bulb zero
def calcwbzlevel(twvals,zvals):
    if max(twvals)<=0:
        wbzlev = 0
    else:
        if 0 in twvals:
            ind=(twvals.tolist()).index(0)
            wbzlev=zvals[ind]
        else:
            ind=len(twvals)-((np.where(np.greater(np.flipud(twvals),0),0,np.flipud(twvals))).tolist()).index(0)
            wbzlev=zvals[ind-1]+(twvals[ind-1]/(twvals[ind-1]-twvals[ind]))*(zvals[ind]-zvals[ind-1])
    return wbzlev

# Interpolate between height levels
def valheight(vals,zvals,height):
    height=float(height)
    if height in zvals:
        val = vals[(zvals.tolist()).index(height)]
    else:
        ind=(np.where(np.greater(zvals,height),0,zvals).tolist()).index(0)
        val = ((height-zvals[ind-1])/(zvals[ind]-zvals[ind-1]))*(vals[ind]-vals[ind-1])+vals[ind-1]
    return val

# Delete grib2 and idx files if they exist
def delhrrrfiles():
    suffixes = ['*grib2*','*.idx']
    for suffix in suffixes:
        filelist = glob.glob(suffix)
        for file in filelist:
            os.remove(file)

def get_hrrr_forecast(forecast_start_time,sitelat,sitelon,siteelev = 2668.0,mlthick = 300,maxprocesses = 10):
    """_summary_

    Args:
        forecast_start_time (_type_): _description_
        sitelat (_type_): _description_
        sitelon (_type_): _description_
        siteelev (float, optional): _description_. Defaults to 2668.0.
        mlthick (int, optional): _description_. Defaults to 300.
        maxprocesses (int, optional): _description_. Defaults to 10.

    Returns:
        _type_: _description_
    """
    # Create global var for the scratch dir
    # Scratch directory to temporarily store HRRR grib files
    scratchdir = './hrrr_scratch/'

    # Make scratch directory if needed
    if not os.path.exists(scratchdir):
        os.makedirs(scratchdir)

    # Get rid of mm ss and extract yr mn dy hr
    run_date = forecast_start_time.strftime('%Y-%m-%d-%H')
    yr,mn,dy,hr = str(run_date).split('-')
    if (hr == '00' or hr == '06' or hr == '12' or hr == '18'):
        maxfhr = 48
    else:
        maxfhr = 18 
    fhrs = tuple(range(maxfhr+1))
    items = [(yr,mn,dy,hr,fhr) for fhr in fhrs]

    # Parallel process if requested
    # Must be run parallel to get output file
    start_time = time.time()

    fhrs = tuple(range(maxfhr+1))
    items = [(yr,mn,dy,hr,fhr,sitelat,sitelon, siteelev, mlthick,scratchdir) for fhr in fhrs]
    processes = min(maxfhr+1, maxprocesses)
    print('Running with '+str(processes)+' processes')
    with Pool(processes=processes) as p:
        output = p.starmap(processhrrr, items)
    columns = ['INIT (YYYYMMDDHH UTC)','FHR','Grid Point Lat','Grid Point Lon','Grid Point Elev (m)','PSFC (PA)','TSFC (K)','T2m (K)','RH2m (%)',
            'Wind Speed 10m (m/s)','Wind Direction 10 m (deg)','Downward Short Wave (W/m2)', 'Upward Short Wave (W/m2)',
            'Downward Long Wave (W/m2)','Upward Long Wave (W/m2)','Water Equiv Precip (mm)',
            'Wet-Bulb Zero Height (m)','Snow-to-Liquid Ratio','Snowfall (cm)']
    sitedf = pd.DataFrame(output, columns=columns)
    sitedf.to_csv('./hrrr_to_snowpack_'+yr+mn+dy+hr+'.csv', index=False)

    # Delete any existing grib2 or idx files
    delhrrrfiles()

    end_time = time.time()
    elapsed_time = end_time - start_time
    print('Elapsed time to get HRRR forcast: ' + str(elapsed_time))
    print('HRRR Processing complete')

    return sitedf

# ------------------ MAIN PROGRAM ----------------
# Defult main runs for Atwater from current time - 1 hour
if __name__ == "__main__":
 
    # Maximum number of parallel processes if being run parallel
    maxprocesses = 10

    # Site coordinates (currently Atwater based on google maps)
    sitelat = 40.591230
    sitelon = -111.637711  #actual Atwater, yields HRRR grid point to east (2928 m elevation)
    #sitelon = -111.660  # slightly down canyon, yields HRRR grid point to west (2825 m elevation)

    # Site elevation (m, currently Atwater based on mesowest)
    siteelev = 2668.0

    # Depth of melting layer below wet-bulb zero (m)
    mlthick = 300

    forecast_start_time = datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1) 
    
    forecast_df = get_hrrr_forecast(forecast_start_time,sitelat,sitelon,siteelev = 2668.0,mlthick = 300,maxprocesses = 10)

    forecast_df.head()

    # -------------- END MAIN PROGRAM ----------------
