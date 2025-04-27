"""
URL utilities for MUR SST data processing.

This module contains functions for generating URLs and listing files.
"""

from datetime import datetime, timedelta
from typing import List
import re
import pandas as pd
pd.set_option('display.max_colwidth', None)

from virtualizarr import open_virtual_dataset
from virtualizarr.codecs import get_codecs

base_url = "s3://podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1"

def make_url(date: datetime) -> str:
    """
    Create an S3 URL for a specific datetime.

    Args:
        date: The datetime to create a URL for

    Returns:
        The S3 URL for the specified datetime
    """
    date_string = date.strftime("%Y%m%d") + "090000"
    components = [
        base_url,
        f"{date_string}-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc",
    ]
    return "/".join(components)


def list_mur_sst_files(start_date: str, end_date: str, dmrpp: bool = True) -> List[str]:
    """
    List all files in S3 with a certain date prefix.

    Args:
        start_date: The start date in YYYY-MM-DD format
        end_date: The end date in YYYY-MM-DD format
        dmrpp: Whether to return DMR++ URLs (default: True)

    Returns:
        A list of S3 URLs for the specified date range
    """
    dates = pd.date_range(start=start_date, end=end_date, freq="1D")
    netcdf_urls = [make_url(date) for date in dates]
    if not dmrpp:
        return netcdf_urls
    return [f + ".dmrpp" for f in netcdf_urls]

def open_virtual_dmrpp(file):
    return open_virtual_dataset(file, indexes={}, filetype='dmrpp')

def extract_date_from_s3_path(s3_path):
    """
    Extracts the date string in YYYY-MM-DD format from the S3 path.
    """
    # Regex to find 8 consecutive digits (YYYYMMDD) in the filename
    match = re.search(r'/(\d{8})\d{6}-', s3_path)
    if match:
        yyyymmdd = match.group(1)
        return f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"
    else:
        return None

def subtract_one_day(date_str):
    """
    Subtracts one day from a date string in YYYY-MM-DD format.
    """
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    new_date = date_obj - timedelta(days=1)
    return new_date.strftime("%Y-%m-%d")        

def process_variable_metadata(vdss, variables, metadata_type='chunks'):
    """
    Process either chunk shapes or codecs from a list of virtual datasets and group them by time periods.
    
    Args:
        vdss: List of virtual datasets
        variables: List of variable names to process
        metadata_type: Type of metadata to process ('chunks' or 'codecs')
                  
    Returns:
        dict: A dictionary where keys are variables and values are dictionaries
              mapping either chunk shapes or codecs to lists of time periods
    """
    metadata_dict = {var: {} for var in variables}
    
    for var in variables:
        # Initialize with first dataset
        if metadata_type == 'chunks':
            current_metadata = vdss[0][var].chunks
        else:  # 'codecs'
            current_metadata = str([c.to_dict() for c in get_codecs(vdss[0][var].data)[1:]])
            
        starting_date = extract_date_from_s3_path(vdss[0][var].data.manifest.dict()['0.0.0']['path'])
        
        # Process each dataset
        for idx, vds in enumerate(vdss):
            if metadata_type == 'chunks':
                this_metadata = vds[var].chunks
            else:  # 'codecs'
                this_metadata = str([c.to_dict() for c in get_codecs(vds[var].data)[1:]])
                
            ending_date = extract_date_from_s3_path(vds[var].data.manifest.dict()['0.0.0']['path'])
            
            # If metadata changed or it's the last dataset
            if this_metadata != current_metadata or idx == len(vdss) - 1:
                # For last dataset, use the actual ending date
                if idx == len(vdss) - 1:
                    time_period = (starting_date, ending_date)
                else:
                    time_period = (starting_date, subtract_one_day(ending_date))
                
                # Add time period to the list for this metadata
                metadata_dict[var].setdefault(current_metadata, []).append(time_period)
                
                # Update for next iteration
                if this_metadata != current_metadata:
                    starting_date = ending_date
                    current_metadata = this_metadata
    
    return metadata_dict

def convert_dict_to_df(data_dict: dict, metadata_name: str):
    rows = []
    for varname, metadata in data_dict.items():
        for metadata_item, date_ranges in metadata.items():
            rows.append({
                'variable': varname,
                metadata_name: metadata_item,
                'date_ranges': date_ranges  # keep the list of periods together
            })
    
    return pd.DataFrame(rows).set_index(['variable', metadata_name])
