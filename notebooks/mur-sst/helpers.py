import boto3
from dask.distributed import Client
from datetime import datetime, timedelta
from dask import compute
import dask.bag as db
import pandas as pd
import icechunk
from s3fs import S3FileSystem
import os
import shutil
from virtualizarr import open_virtual_dataset
import ctypes

bucket = 'podaac-ops-cumulus-protected'
base_url = f"s3://{bucket}/MUR-JPL-L4-GLOB-v4.1"

def make_url(date: datetime) -> str:
    """Create an S3 URL for a specific dateime"""
    date_string = date.strftime("%Y%m%d") + "090000"
    components = [
        base_url, f"{date_string}-JPL-L4_GHRSST-SSTfnd-MUR-GLOB-v02.0-fv04.1.nc"
    ]
    return '/'.join(components)

def list_mur_sst_files(start_date: str, end_date: str):
    """
    list all files in s3 with a certain date prefix
    """
    all_days = pd.date_range(start=start_date, end=end_date, freq="1D")
    return [make_url(d) for d in all_days]
