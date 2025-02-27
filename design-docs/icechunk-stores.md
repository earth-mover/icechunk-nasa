# GPM IMERG Icechunk Dataset Design Document

This document describes the structure of the NASA GPM IMERG dataset and the Icechunk datasets we will be creating from this dataset.

Goals:
- Explore workflows for creating large Icechunk virtual datasets
- Expose and resolve scalability bottlenecks withing Icechunk 
- Understand the performance of both writing and reading Icechunk stores
- Compare virtual store read performance with rechunked native store read performance

## About the Dataset

Official Name: **GPM IMERG Final Precipitation L3 Half Hourly 0.1 degree x 0.1 degree V07 (GPM_3IMERGHH) at GES DISC**

Official NASA Website: https://data.nasa.gov/dataset/GPM-IMERG-Final-Precipitation-L3-Half-Hourly-0-1-d/hqn4-tpfu/about_data

S3 Bucket: `s3://gesdisc-cumulus-prod-protected/GPM_L3/GPM_3IMERGHH.07/`


### Granules

There is one NetCDF4 file produced at every 30-minute interval. The files are named like this:

`s3://gesdisc-cumulus-prod-protected/GPM_L3/GPM_3IMERGHH.07/1998/001/3B-HHR.MS.MRG.3IMERG.19980101-S000000-E002959.0000.V07B.HDF5`


<details>
<summary>Python Function to compute the URL for any Python datetime</summary>

```python
from datetime import datetime, timedelta

base_url = "s3://gesdisc-cumulus-prod-protected/GPM_L3/GPM_3IMERGHH.07"

def make_url(date: datetime) -> str:
    end_date = date + timedelta(minutes=29, seconds=59)
    base_date = datetime(year=date.year, month=date.month, day=date.day, hour=0, minute=0, second=0)
    delta_minutes = (date - base_date) // timedelta(minutes=1)
    components = [
        base_url,
        "{:04d}".format(date.year),
        date.strftime('%j'),  # day of year
        (
            "3B-HHR.MS.MRG.3IMERG." +
            date.strftime("%Y%m%d") +
            "-S" + date.strftime("%H%M%S") +
            "-E" + end_date.strftime("%H%M%S") +
            ".{:04d}".format(delta_minutes) +
            ".V07B.HDF5"
        )
    ]
    return '/'.join(components)
```

</details>

There are 48 files per day.
The dataset begins on Jan. 1, 1998. Through the end of 2024, this amounts to 473,328 files.

### Internal File Structure

The files are HDF5-backed NetCDF4 datasets with two groups:
- `Grid` - this seems to be the primary dataset
- `Grid/Intermediate` - some other variables. :point_left: _We will be ignoring this group for now._

Here are the contents of the `Grid` group

| name | dtype | shape | chunk shape | num. chunks |
|--|--|--|--|--|
| nv | int32 | (2,) | (2,) | 1 |
| lonv | int32 | (2,) | (2,) | 1 |
| latv | int32 | (2,) | (2,) | 1 |
| time | int32 | (1,) | (32,) | 0 |
| lon | float32 | (3600,) | (3600,) | 1 |
| lat | float32 | (1800,) | (1800,) | 1 |
| time_bnds | int32 | (1, 2) | (32, 2) | 0 |
| lon_bnds | float32 | (3600, 2) | (3600, 2) | 1 |
| lat_bnds | float32 | (1800, 2) | (1800, 2) | 1 |
| precipitation | float32 | (1, 3600, 1800) | (1, 145, 1800) | 24 |
| randomError | float32 | (1, 3600, 1800) | (1, 145, 1800) | 24 |
| probabilityLiquidPrecipitation | int16 | (1, 3600, 1800) | (1, 291, 1800) | 12 |
| precipitationQualityIndex | float32 | (1, 3600, 1800) | (1, 145, 1800) | 24 |

The primary data variables are `precipitation`, `randomError`, `probabilityLiquidPrecipitation`, and `precipitationQualityIndex`.
These variables contain **84 chunks**.
These will be the variables in our virtual dataset.

## Overall Approach and Sequencing

We will develop these datasets in the following sequence

- [x] Small-scale (single day) virtual dataset
- [ ] `IN-PROGRESS` Batch-processed virtual dataset of the entire record.
- [ ] `TODO` Incremental appending to virtual Icechunk dataset
- [ ] `TODO` Batch rechunking from virtual to native Icechunk

### Small-scale (single day) virtual dataset

This step allows us to test and verify the basic functionality before moving to the larger-scale data processing problem.
  - https://github.com/earth-mover/icechunk-nasa/blob/main/notebooks/write_virtual.ipynb
  - https://github.com/earth-mover/icechunk-nasa/blob/main/notebooks/read_virtual.ipynb

This step revealed several issues in VirtualiZarr:

- https://github.com/zarr-developers/VirtualiZarr/issues/336 (fixed)
- https://github.com/zarr-developers/VirtualiZarr/issues/341
- https://github.com/zarr-developers/VirtualiZarr/issues/342
- https://github.com/zarr-developers/VirtualiZarr/issues/343

Mostly we were able to move forward by using the `HDFVirtualBackend`.

<details>
<summary>Function to fix fill_value encoding</summary>

```python
from xarray.backends.zarr import FillValueCoder

def fix_ds(ds):
    ds = ds.copy()
    coder = FillValueCoder()
    # promote fill value to attr for zarr V3
    for dvar in ds.data_vars:
        dtype = ds[dvar].dtype
        # this is wrong due to bug in Sean's reader
        #fill_value = dtype.type(ds_concat[dvar].data.zarray.fill_value)
        fill_value = dtype.type(ds[dvar].attrs['CodeMissingValue'])
        encoded_fill_value = coder.encode(fill_value, dtype)
        ds[dvar].attrs['_FillValue'] = encoded_fill_value
```

</details>

### Batch-processed virtual dataset of the entire record

This step allows us to probe the scalability of virtual reference generation and storage in Icechunk.
It involves building a single ARCO data cube from the entire 26-year GPM IMERG record.
The final dataset will have 39,759,552 virtual chunks.

We are currently using `Dask` to parallelize the reading of the files and generation of references. Full notebook [here](https://github.com/earth-mover/icechunk-nasa/blob/main/notebooks/build_virtual_ds_dask.ipynb.
The relevant code looks something like this:

```python
import dask.bag as db
import xarray as xr

def reduce_via_concat(dsets):
    return xr.concat(dsets, dim="time", coords="minimal", join="override")

b = db.from_sequence(all_times, partition_size=48)
all_urls = db.map(make_url, b)
vdsets = db.map(open_virtual, all_urls)
concatted = vdsets.reduction(reduce_via_concat, reduce_via_concat)
```

The `reduction` step allows us to iteratively combine the virtual datasets as they are read.

In the first attempt, we tried to process all of the files in one go.
Dask workers appeared to be running out of memory.
So we switched to the approach of processing one year at a time. In pseudocode:

```python
# special case for first year
ds_1998 = dset_for_year(1998)
ds_1998.virtualize.to_icechunk(ic_repo)

for year in range(1999, 2024):
    ds_year = dset_for_year(year)
    ds_year.virtualize.to_icechunk(ic_repo, append_dim="time")
    cid = ic_repo.commit(f"Appended {year}")
```

We were able to create about 10 years of data this way.
Each subsequent commit required more and more memory, until we eventually ran out of memory.

When opening this store and reading back data, we observed two important performance bottlenecks:
- Calling `group.members()` is very slow, causing `xr.open_dataset` to be slow. According to @dcherian, 
  > yes this is known, `list_dir` is inefficient because it is `list_prefix` with post-filtering.

  We will be fixing this soon.
- Reading any data from arrays (even small ones) is very slow and memory intensive. This is because it requires downloading and loading the entire chunk manifest for the entire dataset.


The size of the chunk manifest appears to be a critical issue at this point. 
For 11 years of data (1998-2008 inclusive), the repo contains 16,972,032 chunks.
This is less than half the total expected number of chunks.
The manifest for this snapshot is 3,086,327,626 bytes (> 3 GB).

Downloading and loading this huge manifest is consuming lots of time and memory.
This also impacts appending, since the dataset must be opened and read by Xarray in order to append.
Ideally, opening the Xarray dataset should _not have any performance characteristics that scale with size of the data variables_.

There are several strategies we could explore to mitigate these issues:
- **Better compression of manifest data.** Our current msgpack format does not use any compression whatsoever. Compressing the manifests will make them faster to download.
- **Concurrent downloading of manifests.** For a 3 GB manifest, splitting the download over many threads will speed it up a lot. (This optimization applies to any file in Icechunk, including chunks.)
- **Manifest sharding.** We can't allow manifests to grow without bound. The Icechunk Spec allows multiple manifests. The question is how do we split them up. This question merits a design doc all of its own. But here a couple of ideas:
  - Define maximum manifest size (e.g. 100 MB)
  - Split manifests by arrays
  - Split manifest by chunk index 
  - Put all of the coordinates in a separate manifest. (Challenge: requires icechunk to "know" what coordinates are. This reaches across several layers of abstraction (Xarray data model -> Zarr data model -> Zarr Store Interface -> Icechunk).

### Incremental appending to virtual Icechunk dataset

We have already begun to explore appending in the previous item.
Making appending work well is contingent on resolving the manifest scalability issues described above.

### Batch rechunking from virtual to native Icechunk

We will work on this after we have a good reference virtual dataset to start from.
