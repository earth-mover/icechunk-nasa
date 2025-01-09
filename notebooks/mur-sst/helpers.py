import boto3
from datetime import datetime, timedelta
from dask import compute
import dask.bag as db
import pandas as pd
from icechunk import IcechunkStore, StorageConfig, StoreConfig, S3Credentials, VirtualRefConfig
from s3fs import S3FileSystem
import os
from virtualizarr import open_virtual_dataset

drop_vars = ['dt_1km_data', 'sst_anomaly']
base_url = "s3://podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1"

def create_virtual_ds(dmrpps: list[str], parallel: bool = True):
    # Note: by changing the backend to dmrpp and using the dmrpp files, we speed up generating the virtual dataset by ~100x.
    # To test that out, you can switch filetype=dmrpp to backend=HDFVirtualBackend or remove it altogether to test the kerchunk backend.
    def open_virtual(f):
        vds = open_virtual_dataset(
            f,
            indexes={},
            # dmrpp doesn't support drop_variables AFAICT
            filetype='dmrpp'
        )
        return vds.drop_vars(drop_vars, errors="ignore")      

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

def find_or_create_icechunk_store(store_name: str = None, store_type: str = 'local', overwrite = True):
    if store_type == "local":
        directory_path = f"./{store_name}"
        storage_config = StorageConfig.filesystem(directory_path)
        virtual_ref_store_config = StoreConfig(
            virtual_ref_config=VirtualRefConfig.s3_from_env(),
        )        
        if overwrite:
            if os.path.exists(directory_path) and os.path.isdir(directory_path):
                shutil.rmtree(directory_path)  # Removes non-empty directories
                print(f"Directory '{directory_path}' and its contents deleted.")
            else:
                print(f"Directory '{directory_path}' does not exist.")    
            store = IcechunkStore.create(
                storage=storage_config, config=virtual_ref_store_config, read_only=False
            )
        else:
            store = IcechunkStore.open_existing(
                storage=storage_config, config=virtual_ref_store_config, read_only=False
            )            
    # Create a session with the EC2 instance's attached role
    if store_type == "s3":
        session = boto3.Session()
        
        # Get the credentials from the session
        credentials = session.get_credentials()
        
        # Extract the actual key, secret, and token
        creds = credentials.get_frozen_credentials()
        storage = StorageConfig.s3_from_config(
            bucket='nasa-veda-scratch',
            prefix=f"icechunk/{store_name}",
            region='us-west-2',
            credentials=S3Credentials(
                access_key_id=creds.access_key,
                secret_access_key=creds.secret_key,
                session_token=creds.token            
            )    
        )
        if overwrite == True:
            store = IcechunkStore.create(
                storage=storage, 
                config=StoreConfig()
            )
        else:
            store = IcechunkStore.open_existing(storage=storage, config=StoreConfig(), read_only=False)
    if store_type == "array_lake":
        from arraylake import Client
        client = Client()
        client.login()
        store = client.get_or_create_repo(f"nasa-impact/{store_name}", kind="icechunk")
    return store

def validate_data(
    store: IcechunkStore,
    dates: list[str],
    fs: S3FileSystem,  
    lat_slice: slice = slice(42, 43),
    lon_slice: slice = slice(-122, -121)
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


def check_codecs(vdss: list):
    from virtualizarr.codecs import get_codecs
    from virtualizarr.manifests.utils import check_same_codecs
    from virtualizarr import zarr
    
    first_codec = get_codecs(vdss[0].analysed_sst.data)
    print(f"first codec: {first_codec}\n")
    
    for vds in vdss:
        codec = get_codecs(vds.analysed_sst.data)
        if codec != first_codec:
            print(codec)
            print(f"{vds.analysed_sst.data.manifest.dict()['0.0.0']['path']}\n")
