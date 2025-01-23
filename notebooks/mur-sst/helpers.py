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
from virtualizarr.zarr import Codec
import ctypes

drop_vars = ['dt_1km_data', 'sst_anomaly']
base_url = "s3://podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1"

def open_virtual(f):
    vds = open_virtual_dataset(
        f,
        indexes={},
        # dmrpp doesn't support drop_variables AFAICT
        filetype='dmrpp'
    )
    return vds.drop_vars(drop_vars, errors="ignore")   

def create_virtual_ds(dmrpps: list[str], parallel: bool = True):
    # Note: by changing the backend to dmrpp and using the dmrpp files, we speed up generating the virtual dataset by ~100x.
    # To test that out, you can switch filetype=dmrpp to backend=HDFVirtualBackend or remove it altogether to test the kerchunk backend.   

    def reduce_via_concat(vdss: list):
        import xarray as xr
        return xr.concat(
            vdss,
            dim="time",
            coords="minimal",
            # not sure why neither of those options work as a replacement for drop_vars in open_virtual (but that's probably a better solution)
            # data_vars="minimal": Only data variables in which the dimension already appears are included.
            # data_vars=list of dims (guessing this should be vars): The listed data variables will be concatenated, in addition to the “minimal” data variables.
            # data_vars=['analysed_sst', 'analysis_error', 'mask', 'sea_ice_fraction'],
            compat="override",
            combine_attrs="override",
        )
        
    if parallel:
        vdss = db.map(open_virtual, db.from_sequence(dmrpps))
        concatted = vdss.reduction(reduce_via_concat, reduce_via_concat)
        return concatted.compute()
    else:
        vdss = [open_virtual(f) for f in dmrpps]
        return reduce_via_concat(vdss)

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

def find_or_create_icechunk_repo(store_name: str = None, store_type: str = 's3', overwrite = True):
    # need to rewrite this for updated icechunk
    if store_type == "local":
        directory_path = f"./{store_name}"
        storage_config = icechunk.local_filesystem_storage(directory_path)
        if overwrite:
            if os.path.exists(directory_path) and os.path.isdir(directory_path):
                shutil.rmtree(directory_path)  # Removes non-empty directories
                print(f"Directory '{directory_path}' and its contents deleted.")
            else:
                print(f"Directory '{directory_path}' does not exist.")
    # Create a session with the EC2 instance's attached role
    elif store_type == "s3":
        session = boto3.Session()
        
        # Get the credentials from the session
        credentials = session.get_credentials()
        
        # Extract the actual key, secret, and token
        creds = credentials.get_frozen_credentials()
        # Note: Storage.new_s3 will erase storage
        storage_config = icechunk.s3_storage(
            bucket='nasa-veda-scratch',
            prefix=f"icechunk/{store_name}",
            region='us-west-2',
            access_key_id=creds.access_key,
            secret_access_key=creds.secret_key,
            session_token=creds.token
        )
    else:
        raise "unsupported store type"
        
    if overwrite == True:
        # This will raise an error
        repo = icechunk.Repository.create(storage=storage_config)
    else:
        repo = icechunk.Repository.open_or_create(storage=storage_config)
    return repo

def validate_data(
    store: icechunk.Repository,
    dates: list[str],
    fs: S3FileSystem,  
    lat_slice: slice = slice(48, 49),
    lon_slice: slice = slice(-125, -124)
): 
    import xarray as xr
    time_slice = slice(*dates)
    
    print(f"Open icechunk store...")
    xds = xr.open_zarr(store, consolidated=False)
    
    print("Computing icechunk store result...")
    icechunk_result = xds.analysed_sst.sel(lat=lat_slice, lon=lon_slice, time=time_slice).mean().values
    print(f"Icechunk store result: {icechunk_result}")
    
    print("Opening original files...")
    og_files = list_mur_sst_files(start_date=dates[0], end_date=dates[1])
    og_ds = xr.open_mfdataset([fs.open(f) for f in og_files], drop_variables=drop_vars, parallel=True)
    
    print("Computing original files result")
    og_result = og_ds.analysed_sst.sel(lat=lat_slice, lon=lon_slice, time=time_slice).mean().values
    print(f"Result from original files: {og_result}")
    assert og_result == icechunk_result

expected_codec = Codec(compressor=None, filters=[{'id': 'shuffle', 'elementsize': 2}, {'id': 'zlib', 'level': 6}])
def check_codecs(vdss: list, expected: Codec = expected_codec):
    from virtualizarr.codecs import get_codecs
    from virtualizarr.manifests.utils import check_same_codecs
    from virtualizarr import zarr
    
    for vds in vdss:
        codec = get_codecs(vds.analysed_sst.data)
        if codec != expected:
            print(codec)
            print(f"{vds.analysed_sst.data.manifest.dict()['0.0.0']['path']}\n")

def trim_dask_worker_memory(client: Client):
    def trim_memory() -> int:
        libc = ctypes.CDLL("libc.so.6")
        return libc.malloc_trim(0)
    
    return client.run(trim_memory)
