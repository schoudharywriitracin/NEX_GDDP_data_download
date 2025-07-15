# -*- coding: utf-8 -*-
"""
Created on Mon Aug 12 14:59:00 2024

@author: SOURAV
"""

import os
import wget
from mpi4py import MPI

# INPUT SECTION
"----------------------------------------------------------------------------------------------------------------------"
models = {
    
    "CESM2": {"param1": "r4i1p1f1", "param2": "gn"},
    "CESM2-WACCM" : {"param1": "r3i1p1f1", "param2": "gn"},
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
    "BCC-CSM2-MR" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "CMCC-CM2-SR5" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "CMCC-ESM2" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "CanESM5" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "IITM-ESM" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "MPI-ESM1-2-HR" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "NorESM2-LM" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "NorESM2-MM" : { "param1": "r1i1p1f1" , "param2": "gn" }, 
    "TaiESM1" : { "param1": "r1i1p1f1" , "param2": "gn" }, 

}
scenarios = ["historical"]
variables = ["pr"]

"----------------------------------------------------------------------------------------------------------------------"

def download_netcdf(model, scenario, variable, year, params):
    param1 = params.get("param1")
    param2 = params.get("param2")

    base_url = f"https://ds.nccs.nasa.gov/thredds/ncss/grid/AMES/NEX/GDDP-CMIP6/{model}/{scenario}/{param1}/{variable}/{variable}_day_{model}_{scenario}_{param1}_{param2}_{year}.nc"
    start_time, end_time = ("1950-01-01T12:00:00Z", "2014-12-31T12:00:00Z") if scenario == "historical" else ("2015-01-01T12:00:00Z", "2100-12-31T12:00:00Z")
    query_params = {"var": variable, "north": "21", "west": "73", "east": "77", "south": "18", "horizStride": "1", "time_start": start_time, "time_end": end_time, "accept": "netcdf3", "addLatLon": "true", "year": year}
    url = base_url + "?" + "&".join([f"{key}={value}" for key, value in query_params.items()])
    output_directory = os.path.join("F:/data1/Climate_datasets/NEX_GDDP/NEX-GDDP_stn_wise_uppr_god/MY_model", model, scenario, variable)
    os.makedirs(output_directory, exist_ok=True)

    try:
        filename = wget.download(url, out=output_directory)
        print(f"File downloaded: {filename}")
    except Exception as e:
        print(f"An error occurred: {e}")

# MPI initialization
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

tasks = []

for model, params in models.items():
    for scenario in scenarios:
        for variable in variables:
            if scenario == "historical":
                year_range = range(1950, 2015)
            else:
                year_range = range(2015, 2101)

            for year in year_range:
                task = (model, scenario, variable, year, params)
                tasks.append(task)

# Distribute tasks across nodes
tasks_per_node = len(tasks) // size
tasks_for_this_node = tasks[rank * tasks_per_node:(rank + 1) * tasks_per_node]

# Handle any remaining tasks
if rank == size - 1:
    tasks_for_this_node += tasks[size * tasks_per_node:]

# Sequentially download the tasks assigned to this node
for task in tasks_for_this_node:
    download_netcdf(*task)

# Finalize MPI (optional, typically done automatically)
MPI.Finalize()