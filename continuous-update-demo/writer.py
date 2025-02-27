import icechunk as ic
import zarr
import boto3
import numpy as np
import xarray as xr
from rich.console import Console
from rich.panel import Panel
from time import perf_counter

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
    
    store_name = "continuous-update-demo-0"
    session = boto3.Session()

    # Get the credentials from the session
    credentials = session.get_credentials()
    
    # Extract the actual key, secret, and token
    creds = credentials.get_frozen_credentials()
    storage = ic.s3_storage(
        bucket='nasa-veda-scratch',
        prefix=f"icechunk/{store_name}",
        access_key_id=creds.access_key,
        secret_access_key=creds.secret_key,
        session_token=creds.token            
    )

    return ic.Repository.open_or_create(storage)


def get_fragment():
    return xr.DataArray(
        np.random.rand(nt, ny, nx),
        dims=("time", "lat", "lon"),
        name="precipitation"
    ).to_dataset()
            

def write_data(append=False):
    ds = get_fragment()
    encoding = {
        v: {"chunks": chunk_shape}
        for v in ds
    }
    repo = get_repo()
    session = repo.writable_session("main")
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
    return ds


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
    
    console.print(Panel("[red] Writer initialized"))

    console.log("Writing initial dataset")
    try:
        ds = write_data()
    except FileExistsError:
        ds = write_data(append=True)
    console.log(ds)
    for n in range(100):
        tic = perf_counter()
        write_data(append=True) 
        delta_t = perf_counter() - tic
        console.log(f"Wrote update {n} in {delta_t:.2f}")

        if n > 0 and n % 10 == 0:
            console.log("[yellow]Collecting garbage")
            gc_result = garbage_collect()
            console.log({k: getattr(gc_result, k) for k in gc_attrs})
            
            


if __name__ == "__main__":
    main()

