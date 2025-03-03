import icechunk as ic
import zarr
import boto3
import numpy as np
import xarray as xr
from rich.console import Console
from rich.panel import Panel
from time import perf_counter
import s3fs
import sys
import zarr

session = boto3.Session()

# Get the credentials from the session
credentials = session.get_credentials()
creds = credentials.get_frozen_credentials()
s3 = s3fs.S3FileSystem(
    key=creds.access_key,
    secret=creds.secret_key,
    token=creds.token    
)

    
store_name = "continuous-update-demo-0"
bucket = 'nasa-veda-scratch'
prefix = f"icechunk/{store_name}"

console = Console()

zarr.config.set(
    {
        'threading.max_workers': 16,
        'async.concurrency': 32
    }
)


nt, ny, nx = 1, 180, 360 
chunk_shape = (100, 10, 10)


def get_repo():


    
    # Extract the actual key, secret, and token

    storage = ic.s3_storage(
        bucket=bucket,
        prefix=prefix,
        access_key_id=creds.access_key,
        secret_access_key=creds.secret_key,
        session_token=creds.token            
    )

    return ic.Repository.open_or_create(storage)


def get_fragment(n):
    data = np.sin(2 * np.pi * n / 20) + np.random.rand(nt, ny, nx)/3
    return xr.DataArray(
        data,
        dims=("time", "lat", "lon"),
        name="precipitation"
    ).to_dataset()
            

def write_data():

    repo = get_repo()
    session = repo.writable_session("main")

    try:
        array = zarr.open_array(session.store, path="precipitation", mode="r")
        nt, _, _ = array.shape
        append = True
    except FileNotFoundError:
        append = False
        nt = 0
    
    ds = get_fragment(nt)
    encoding = {
        v: {"chunks": chunk_shape}
        for v in ds
    }

    if append:
        kwargs = {"append_dim": "time", "mode": "a"}
    else:
        kwargs = {"encoding": encoding}
    
    ds.to_zarr(
        session.store,
        zarr_format=3,
        consolidated=False,
        **kwargs
    )
    session.commit("wrote initial data")
    return nt, ds


def garbage_collect():
    repo = get_repo()
    for hist in repo.ancestry(branch="main"):
        break
    latest = hist.written_at
    exp_result = repo.expire_snapshots(latest)
    gc_result = repo.garbage_collect(latest)
    return gc_result


gc_attrs = (
    "chunks_deleted", "manifests_deleted", "snapshots_deleted"
)


def main():

    argv = sys.argv
    delete = (len(argv) > 1) and argv[1] == "--delete"
    
    console.print(Panel("[red] Writer initialized"))

    if delete:
        console.log("Deleting existing data")
        s3.rm(f"{bucket}/{prefix}", recursive=True)
    
    console.log("Writing initial data")
    nt, ds = write_data()
    console.log(f"Found [yellow]{nt}[/] timesteps")
    console.log(ds)
    for n in range(100):
        tic = perf_counter()
        nt, _ = write_data() 
        delta_t = perf_counter() - tic
        console.log(f"Wrote update [yellow]{nt}[/] in {delta_t:.2f} s")

        if n > 0 and n % 10 == 0:
            console.log("[yellow]Collecting garbage")
            gc_result = garbage_collect()
            console.log({k: getattr(gc_result, k) for k in gc_attrs})
            
            


if __name__ == "__main__":
    main()

