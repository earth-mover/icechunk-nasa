# MUR SST Icechunk Dataset Design Document

This document describes the structure of the NASA MUR SST dataset and the Icechunk datasets we will be creating from this dataset.

Goals:

- Explore workflows for creating large Icechunk virtual datasets
- Expose and workaround issues in the underlying data files
- Expose and resolve scalability bottlenecks withing Icechunk
- Understand the performance of both writing and reading Icechunk stores
- Compare virtual store read performance with rechunked native store read performance

## About the Dataset

Official Name: **GHRSST Level 4 MUR Global Foundation Sea Surface Temperature Analysis (v4.1) at PO.DAAC**

Official NASA Website: http://podaac.jpl.nasa.gov/Multi-scale_Ultra-high_Resolution_MUR-SST

S3 Bucket: `s3://podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1`

### Granules

There is one NetCDF4 file produced every day. See `helpers.make_url` function to see how files are named.

The dataset begins on June 1, 2002. Through the end of 2024, this amounts to ~8,249 files.

### Internal File Structure

The files are standard NetCDF4 with 4 data variables across all files, and 2 additional variables which show up later in the dataset. Since variables need to be consistent across dimensions, these variables (`dt_1km_data`, `sst_anomaly`) are removed.

The other variables are listed below:

| name             | dtype   | shape             | chunk shape     | num. chunks |
| ---------------- | ------- | ----------------- | --------------- | ----------- |
| time             | int32   | (1,)              | (1,)            | 1           |
| lon              | float32 | (36000,)          | (36000,)        | 1           |
| lat              | float32 | (17999,)          | (17999,)        | 1           |
| analysed_sst     | int32   | (1, 17999, 36000) | (1, 1023, 2047) | 324         |
| analysis_error   | float32 | (1, 17999, 36000) | (1, 1023, 2047) | 324         |
| mask             | float32 | (1, 17999, 36000) | (1, 1447, 2895) | 169         |
| sea_ice_fraction | float32 | (1, 17999, 36000) | (1, 1447, 2895) | 169         |

## Issues with the data and how we plan to overcome them:

1. As mentioned above, some files contain the extra variables `dt_1km_data` and `sst_anomaly` (I believe the started appear in 2014, need to double check this). We will remove these variables from the dataset.
2. In 2003 and 2021, a few files contain a different encoding. These files are written as native Zarr.
3. Starting in 2022 and again in 2023, a few days have a different chunk shape. This chunk shape seems to become the default on 2023-09-04. As for the different encoding, we will handle the days in before 2023-09-04 as special cases and write those days as native Zarr. For the days after 2023-09-04, we will create a new virtual dataset. This will become the ongoing dataset for new data.

Regarding writing data as native zarr, one limitation in the current implementation is it appears the use of dask is not permitted as it results in `ValueError: store error: cannot write to read-only store`. There appears to be a resolution to this in https://icechunk.io/icechunk-python/examples/dask_write but requires and upgrade to icechunk icechunk-v0.1.0-alpha.8.

## Overall Approach and Sequencing

We will develop these datasets in the following sequence

- [ ] `IN-PROGRESS` Complete virtual dataset from 2002-06-02 to 2023-09-04.
- [ ] `TODO` Complete virtual dataset from 2023-09-05 to present day.
- [ ] `TODO` Incremental appending to the second virtual Icechunk dataset.
- [ ] `TODO` Batch rechunking from virtual to native Zarr, stored in Icechunk.

### Complete virtual dataset from 2002-06-02 to 2023-09-04.

See notebooks/mur-sst/write_virtual.ipynb for the code. Year by year, it uses dask to parallelize generation of virtual datasets, writes those datasets to the icechunk store and then uses dask to validate the data by generating a mean over a year for a specific lat,lon degree location from the Icechunk store and using the original files.

This work also exposed an issue with `_FillValue` in VirtualiZarr's dmrpp reader: https://github.com/zarr-developers/VirtualiZarr/pull/369.

### Complete virtual dataset from 2023-09-05 to present day.

TODO

### Incremental appending to virtual Icechunk dataset

TODO

### Batch rechunking from virtual to native Icechunk

TODO
