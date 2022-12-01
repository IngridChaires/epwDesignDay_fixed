#!/usr/bin/env python
# coding: utf-8

from os import path
import os
import pandas as pd
import requests, zipfile, io, os
from zipfile import BadZipfile
from zipfile import ZipFile
import shutil
import glob
import numpy as np
#pd.set_option('display.max_colwidth', -1)
df = pd.read_csv('AllRegion_EPWLinks.csv', index_col=0)
#df['zip'] = df['URL for files'].str.split('/').str[-1]
df['WMO'] = df['URL for files'].str.split('/').str[-1].str.extract('(\d+)')
df['WMO'] = df['WMO'].fillna(0)
df['WMO'] =df['WMO'].astype(str).astype(int)
df['last_year'] = df['URL for files'].str.split('/').str[-1].str.split('.').str[-2].str.split('-').str[-1]
df['last_year'] = df['last_year'].astype('string')
df['last_year'] = df['last_year'].str[-4:]

def download_file(url):
    headers=   {"Auth": "{abcd}",
                "accept": "*/*",
               "accept-encoding": "gzip;deflate;br" }
    response = requests.request("GET", url, headers = headers)
    filename = os.getcwd()+'/downloads_EPW/'
    #filename = os.getcwd()+'/test_epw/'
    try:
        with ZipFile(io.BytesIO(response.content)) as z:
            print("zipfile is OK")
            #z = zipfile.ZipFile(io.BytesIO(response.content))
            z_files = z.namelist()
            #print(z_files)
            extension = '.epw'
            epw = [file for file in z_files if os.path.splitext(file)[1] == extension]
            epwName= epw[0]
            print(epwName)
            z.extract(epw[0],filename)
            #print(epw[0])
            #print(filename)
            #z.extractall(filename)
    except BadZipfile:
        print('Download did not work')
        epwName = "N/A"
    return response.status_code, epwName


## Read in an EPW file to a Pandas dataframe
def readEPW(path):
    # Read the location
    df = pd.read_csv(path, nrows=1, header=None,encoding_errors='ignore') # Read only the first row
    latitude = df.iloc[0, 6]
    longitude = df.iloc[0, 7]
    timezone = df.iloc[0, 8]

    # Read the hourly data
    df = pd.read_csv(path, skiprows=range(8), header=None,encoding_errors='ignore') # Read the file, skip the 8 header rows
    # There are no column names in the file. We need to assign them.
    df.columns = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Data Source and Uncertainty Flags',
                  'Dry Bulb Temperature', 'Dew Point Temperature', 'Relative Humidity', 'Atmospheric Station Pressure',
                  'Extraterrestrial Horizontal Radiation', 'Extraterrestrial Direct Normal Radiation', 'Horizontal Infrared Radiation Intensity',
                  'Global Horizontal Radiation', 'Direct Normal Radiation', 'Diffuse Horizontal Radiation',
                  'Global Horizontal Illuminance', 'Direct Normal Illuminance', 'Diffuse Horizontal Illuminance', 'Zenith Luminance',
                  'Wind Direction', 'Wind Speed', 'Total Sky Cover', 'Opaque Sky Cover', 'Visibility', 'Ceiling Height',
                  'Present Weather Observation', 'Present Weather Codes', 'Precipitable Water', 'Aerosol Optical Depth',
                  'Snow Depth', 'Days Since Last Snowfall', 'Albedo', 'Liquid Precipitation Depth', 'Liquid Precipitation Quantity']
    return df

## Read in an EPW file to a Pandas dataframe
def readEPW_DesignDay(path):
    # Read the location
    df = pd.read_csv(path, nrows=1, header=None,encoding_errors='ignore') # Read only the first row
    latitude = df.iloc[0, 6]
    longitude = df.iloc[0, 7]
    timezone = df.iloc[0, 8]

    # Read the hourly data
    df = pd.read_csv(path, skiprows=range(8), header=None,encoding_errors='ignore') # Read the file, skip the 8 header rows
    # There are no column names in the file. We need to assign them.
    df.columns = ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Data Source and Uncertainty Flags',
                  'Dry Bulb Temperature', 'Dew Point Temperature', 'Relative Humidity', 'Atmospheric Station Pressure',
                  'Extraterrestrial Horizontal Radiation', 'Extraterrestrial Direct Normal Radiation', 'Horizontal Infrared Radiation Intensity',
                  'Global Horizontal Radiation', 'Direct Normal Radiation', 'Diffuse Horizontal Radiation',
                  'Global Horizontal Illuminance', 'Direct Normal Illuminance', 'Diffuse Horizontal Illuminance', 'Zenith Luminance',
                  'Wind Direction', 'Wind Speed', 'Total Sky Cover', 'Opaque Sky Cover', 'Visibility', 'Ceiling Height',
                  'Present Weather Observation', 'Present Weather Codes', 'Precipitable Water', 'Aerosol Optical Depth',
                  'Snow Depth', 'Days Since Last Snowfall', 'Albedo', 'Liquid Precipitation Depth', 'Liquid Precipitation Quantity']

    htg_996_epw= df['Dry Bulb Temperature'].quantile(q=0.004)
    htg_990_epw= df['Dry Bulb Temperature'].quantile(q=0.010)
    clg_004_epw= df['Dry Bulb Temperature'].quantile(q=0.996)
    clg_010_epw= df['Dry Bulb Temperature'].quantile(q=0.99)
    return htg_996_epw,htg_990_epw,clg_004_epw,clg_010_epw

#import DD data
df_DD = pd.read_csv('Climate Data 2021_SI_Rev.csv')
df_summary = df.merge(df_DD, how='inner', on='WMO')

s_htg_996_epw= []
s_htg_990_epw= []
s_clg_004_epw= []
s_clg_010_epw= []

counter =-1
#for url in df_summary['URL for files']:
for index in df_summary.index:
    if index > counter:
        #print(index)
        print(counter)
        counter=counter+1
        url = df_summary.loc[index,'URL for files']
        print(url)
        status, epwpath = download_file(url)
        print(status, epwpath)
        if status ==  200:
            htg_996,htg_990,clg_004,clg_010 = readEPW_DesignDay(path.join('downloads_EPW/',epwpath))
            #print(htg_996,htg_990,clg_004,clg_010)
            s_htg_996_epw.append(htg_996)
            s_htg_990_epw.append(htg_990)
            s_clg_004_epw.append(clg_004)
            s_clg_010_epw.append(clg_010)
            os.remove(path.join('downloads_EPW/',epwpath))
        else:
            s_htg_996_epw.append("N/A")
            s_htg_990_epw.append("N/A")
            s_clg_004_epw.append("N/A")
            s_clg_010_epw.append("N/A")
        continue

df_summary['Heating DB 99.6% - EPW'] = s_htg_996_epw
df_summary['Heating DB 99% - EPW'] = s_htg_990_epw
df_summary['Cooling DB 0.4% - EPW'] = s_clg_004_epw
df_summary['Cooling DB 1% - EPW'] = s_clg_010_epw

filename = 'AllRegion_Results.csv'
export_csv = df_summary.to_csv(filename, index = None, header=True)
