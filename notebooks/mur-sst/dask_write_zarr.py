import helpers
import icechunk
import argparse
import pandas as pd
import zarr
from s3fs import S3FileSystem
import xarray as xr
from dask.distributed import Client
import numpy as np
from dataclasses import dataclass
from typing import cast
# Adapted from https://icechunk.io/icechunk-python/examples/dask_write/#icechunk.Conflict.path

@dataclass
class Task:
    """A task distributed to Dask workers"""
    session: icechunk.Session # The worker will use this Icechunk session to read/write to the dataset
    time: int # The position in the coordinate dimension where the read/write should happen
    var: str
    data_array: xr.DataArray

    def write(self) -> None:
        """Write the data array to the dataset"""
        store = self.session.store
        group = zarr.group(store=store, overwrite=False)
        current_array = cast(zarr.Array, group[self.var])
        current_array[self.time, :, :] = self.data_array.values
        return self.session

def update(
        repo: icechunk.Repository,
        start_date: str,
        end_date: str,
        fs: S3FileSystem,
        client: Client
    ) -> None:
    """
    (1) determine list of dates from range provided in list arg
    (2) resize the zarr array with length of dates array
    (3) write the dates array to the zarr 'time' array
    (4) Use dask to open a dataset with all dates
    (5) for each variable and each time, create a task
    (6) execute a write task using dask
    """
    session = repo.writable_session("main")
    store = session.store
    group = zarr.group(store=store, overwrite=False)

    # TODO: A lot of this is custom to MUR SST, would be nice to make it re-usable
    data_vars = ["analysed_sst", "analysis_error", "mask", "sea_ice_fraction"]
    dt_index = pd.date_range(start=start_date, end=end_date, freq="1D")
    num_days = len(dt_index)
    # current shape
    current_time_shape = group["time"].shape
    group["time"].resize(current_time_shape[0] + num_days)
    reference_date = pd.Timestamp('1981-01-01 00:00:00')
    dt_index_seconds_since_1981 = (dt_index - reference_date).total_seconds()
    group["time"][-num_days:] = np.int32(dt_index_seconds_since_1981)

    # Open the dataset with all dates
    files = helpers.list_mur_sst_files(start_date=start_date, end_date=end_date)
    print("Opening files")
    ds = xr.open_mfdataset(
        [fs.open(f) for f in files],
        parallel=True,
        mask_and_scale=False,
        drop_variables=['dt_1km_data', 'sst_anomaly']
    )
    ds['analysed_sst'] = ds['analysed_sst'].chunk({'time': 1, 'lat': 1023, 'lon': 2047})
    ds['analysis_error'] = ds['analysis_error'].chunk({'time': 1, 'lat': 1023, 'lon': 2047})
    ds['mask'] = ds['mask'].chunk({'time': 1, 'lat': 1447, 'lon': 2895})
    ds['sea_ice_fraction'] = ds['sea_ice_fraction'].chunk({'time': 1, 'lat': 1447, 'lon': 2895})    
    print("Files opened")

    # Create tasks for each variable and each time
    tasks = []
    for var in data_vars:
        current_shape = group[var].shape
        # TODO: This assumes the time dimension comes first, this should be configurable
        group[var].resize((current_shape[0] + num_days, current_shape[1], current_shape[2]))
        for time_idx, datetime in enumerate(dt_index):
            data_array = ds[var].sel(time=datetime)
            tasks.append(Task(session=session, time=current_shape[0] + time_idx, var=var, data_array=data_array))

    # Execute the write tasks
    print("Mapping write tasks to dask client")
    map_result = client.map(Task.write, tasks)
    worker_sessions = client.gather(map_result)
    
    print("Starting distributed commit")
    for worker_session in worker_sessions:
        session.merge(worker_session)
    commit_res = session.commit(f"Distributed commit for {start_date} to {end_date}")
    assert commit_res
    print("Distributed commit done")

