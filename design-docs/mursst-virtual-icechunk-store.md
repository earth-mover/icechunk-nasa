# MUR SST Icechunk Dataset Design Document

This document describes the structure of the NASA MUR SST dataset and the Icechunk datasets we will be creating from this dataset.

Goals:

- Share workflow for creating an icechunk virtual dataset from the MUR SST dataset.
- Expose and workaround issues in the underlying data files.
- Understand the performance of both writing and reading Icechunk stores.
- Compare virtual store read performance with reading from the original files.

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

Most issues encountered relate to variation across files -- and thus variation across the time dimension -- in variables, chunk shape and encoding. Currently, multi-dimensional zarr arrays are expected to have uniformity in these aspects.

1. **Extra variables:** Some files contain the extra variables `dt_1km_data` and `sst_anomaly`. These variables are dropped.
2. **Different encodings:** The standard encoding for data variables appears to be `[{'id': 'shuffle', 'elementsize': 2}, {'id': 'zlib', 'level': 6}]`. However, in 2003, 2021, and 2022 a few files contain a different encoding. These files are written as native Zarr.
3. **Different chunk shapes:** Starting in 2023, some files have different chunk shapes.

   From 2023-02-24 to 2023-02-28, on 2023-04-22, and 2023-09-04 to 2024-03-23:

   - `analysed_sst` and `analysis_error` use a chunk shape `(1, 3600, 7200)`. The original chunk shape was `(1, 1023, 2047)`.
   - `sea_ice_fraction` and `mask` use a chunk shape `(1, 4500, 9000)`. The original chunk shape was `(1, 1447, 2895)`.

   2024-03-24 to date:

   - `analysed_sst` and `analysis_error` use a chunk shape `(1, 1023, 2047)`. This is the original chunk shape.
   - `sea_ice_fraction` and `mask` use a chunk shape `(1, 1023, 2047)`. **Note:** This shape has never been used for these variables before.

   Since the different chunk shapes only appear a few times before 2023-09-04, we write those files as native Zarr.

   It is yet to be determined how we will handle the chunk shape changes after 2023-09-04.

## Overall Approach and Sequencing

We will develop these datasets in the following sequence

- [x] Complete virtual dataset from 2002-06-02 to 2023-09-03.
- [x] Demonstrate how to read from the virtual dataset.
- [ ] Determine how to handle creating a virtual dataset from 2023-09-04 to present day.
- [ ] Incremental appending to the second virtual Icechunk dataset.
- [ ] Batch rechunking from virtual to native Zarr, stored in Icechunk.

### Development Environment

The notebooks in [notebooks/mur-sst](../notebooks/mur-sst) was executed on the [VEDA JupyterHub](https://hub.openveda.cloud) using a custom image (quay.io/developmentseed/veda-optimized-data-delivery-image:latest) maintained in https://github.com/developmentseed/veda-optimized-data-delivery-image.

> [!NOTE]  
> The latest version of this image uses a custom branch of [VirtualiZarr](https://github.com/developmentseed/veda-optimized-data-delivery-image/blob/main/Dockerfile#L45).

> [!WARNING]
> As icechunk is still in development, it is required that the same version of icechunk is used for writing and reading the virtual dataset. The version used for writing this dataset was `0.1.0-alpha12`.

### Writing the virtual dataset from 2002-06-02 to 2023-09-03

See [notebooks/mur-sst/write_virtual-2002-2023.ipynb](../notebooks/mur-sst/write_virtual-2002-2023.ipynb). Year by year, it uses dask to parallelize generation of virtual datasets, writes those datasets to the icechunk store and then uses dask to validate the data by generating a mean over a year for a specific location from the Icechunk store and then doig the same using the original files.

### Reading from the virtual dataset

See [notebooks/mur-sst/read_virtual.ipynb](../notebooks/mur-sst/read_virtual.ipynb). This notebook demonstrates how to read from the virtual dataset using both xarray and Zarr.

### Complete virtual dataset from 2023-09-04 to present day.

TBD

### Incremental appending to virtual Icechunk dataset

TBD

### Batch rechunking from virtual to native Icechunk

TBD
