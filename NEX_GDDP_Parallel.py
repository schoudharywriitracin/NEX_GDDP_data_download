# -*- coding: utf-8 -*-
"""
Created on Tue Dec 19 17:08:28 2023

@author: SOURAV
"""

import os
import wget
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# INPUT SECTION
"----------------------------------------------------------------------------------------------------------------------"
# Example usage with lists
#Models with different parmaters
models = {
    "CNRM-ESM2-1": {"param1": "r1i1p1f2", "param2": "gr"},
    "CESM2": {"param1": "r4i1p1f1", "param2": "gn"},
    "CESM2-WACCM" : {"param1": "r3i1p1f1", "param2": "gn"},
    "CNRM-CM6-1" : {"param1": "r1i1p1f2", "param2": "gr"},
    "EC-Earth3" : {"param1": "r1i1p1f1", "param2": "gr"},
    "EC-Earth3-Veg-LR" : {"param1": "r1i1p1f1", "param2": "gr"},
    "FGOALS-g3" : {"param1": "r3i1p1f1", "param2": "gn"},
    "GFDL-CM4" : {"param1": "r1i1p1f1", "param2": "gr1"},
    "GFDL-CM4_gr2" : {"param1": "r1i1p1f1", "param2": "gr2"},
    "GFDL-ESM4" : {"param1": "r1i1p1f1", "param2": "gr1"},
    "GISS-E2-1-G" : {"param1": "r1i1p1f2", "param2": "gn"},
    "HadGEM3-GC31-LL" :  {"param1": "r1i1p1f3", "param2": "gn"},
    "INM-CM4-8" : {"param1": "r1i1p1f1", "param2": "gr1"},
    "INM-CM5-0" : {"param1": "r1i1p1f1", "param2": "gr1"},
    "IPSL-CM6A-LR" : {"param1": "r1i1p1f1", "param2": "gr"},
    "KACE-1-0-G" : {"param1": "r1i1p1f1", "param2": "gr"},
    "KIOST-ESM" : {"param1": "r1i1p1f1", "param2": "gr1"},
    "MIROC-ES2L" : {"param1": "r1i1p1f2", "param2": "gn"},
    "UKESM1-0-LL" : {"param1": "r1i1p1f2", "param2": "gn"},
    "ACCESS-CM2" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "ACCESS-ESM1-5" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "BCC-CSM2-MR" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "CMCC-CM2-SR5" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "CMCC-ESM2" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "CanESM5" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "IITM-ESM" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "MIROC6" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "MPI-ESM1-2-HR" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "MPI-ESM1-2-LR" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "MRI-ESM2-0" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "NESM3" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "NorESM2-LM" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "NorESM2-MM" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "TaiESM1" : { "param1": "r1i1p1f1" , "param2": "gn" }, 

}
#Models with same parameters of "param1": "r1i1p1f1", "param2": "gn"
#models = ["ACCESS-CM2","ACCESS-ESM1-5","BCC-CSM2-MR","CMCC-CM2-SR5","CMCC-ESM2","CanESM5"," IITM-ESM","IPSL-CM6A-LR","MIROC6","MPI-ESM1-2-HR","MPI-ESM1-2-LR","MRI-ESM2-0","NESM3","NorESM2-LM","NorESM2-MM","TaiESM1"]
scenarios = ["historical", "ssp126", "ssp245", "ssp370", "ssp585"]
variables = ["pr", "tasmin", "tasmax"]



models = { "MPI-ESM1-2-LR" : { "param1": "r1i1p1f1" , "param2": "gn" }}
scenarios = ["historical"]
variables = ["pr"]


"----------------------------------------------------------------------------------------------------------------------"

def download_netcdf(model, scenario, variable, year, params):
     
    #model_params = params.get(model, {})  # Get params for the specific model, default to an empty dictionary if not present
    param1 = params.get("param1")
    param2 = params.get("param2")

    base_url = f"https://ds.nccs.nasa.gov/thredds/ncss/grid/AMES/NEX/GDDP-CMIP6/{model}/{scenario}/{param1}/{variable}/{variable}_day_{model}_{scenario}_{param1}_{param2}_{year}.nc"
    start_time, end_time = ("1950-01-01T12:00:00Z", "2014-12-31T12:00:00Z") if scenario == "historical" else ("2015-01-01T12:00:00Z", "2100-12-31T12:00:00Z")
    query_params = {"var": variable, "north": "21", "west": "73", "east": "77", "south": "18", "horizStride": "1", "time_start": start_time, "time_end": end_time, "accept": "netcdf3", "addLatLon": "true", "year": year}
    url = base_url.format(model=model, scenario=scenario, variable=variable, year=year) + "?" + "&".join([f"{key}={value}" for key, value in query_params.items()])
    output_directory = os.path.join("F:/data1/Climate_datasets/NEX_GDDP/NEX-GDDP_stn_wise_uppr_god/MY_model", model, scenario, variable)
    os.makedirs(output_directory, exist_ok=True)
    
    try:
        filename = wget.download(url, out=output_directory)
        print(f"File downloaded: {filename}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Function to download in parallel
def download_netcdf_parallel(task):
    download_netcdf(*task)

tasks = []

for model, params in models.items():
    for scenario in scenarios:
        for variable in variables:
            if scenario == "historical":
                year_range = range(1950, 2015)
            else:
                year_range = range(2015, 2101)
            
            # Create directory for model, scenario, and variable
            model_scenario_variable_directory = os.path.join("F:/data1/Climate_datasets/NEX_GDDP/NEX-GDDP_stn_wise_uppr_god/MY_model", model, scenario, variable)
            os.makedirs(model_scenario_variable_directory, exist_ok=True)

            for year in year_range:
                task = (model, scenario, variable, year, params)
                tasks.append(task)

# Use ThreadPoolExecutor for parallel processing
with ThreadPoolExecutor() as executor:
    executor.map(download_netcdf_parallel, tasks)
