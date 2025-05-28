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
from typing import Optional

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

def get_repo(bucket_name: str, store_name: str, ea_creds: Optional[dict] = None):
    storage = icechunk.s3_storage(
        bucket=bucket_name,
        prefix=f"icechunk/{store_name}",
        anonymous=True
    )

    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(icechunk.VirtualChunkContainer("s3", "s3://", icechunk.s3_store(region="us-west-2")))

    repo_config = dict(
        storage=storage,
        config=config,
    )
    if ea_creds:
        earthdata_credentials = icechunk.containers_credentials(
            s3=icechunk.s3_credentials(
                access_key_id=ea_creds['accessKeyId'],
                secret_access_key=ea_creds['secretAccessKey'],
                session_token=ea_creds['sessionToken']
            )
        )
        repo_config['virtual_chunk_credentials'] = earthdata_credentials
    return icechunk.Repository.open(**repo_config)
