# MUR SST Icechunk Dataset Design Document

## Executive Summary

We created a virtual Icechunk dataset for the MUR SST dataset, enabling efficient cloud-based access while handling inconsistencies in variables, encoding, and chunking.

The workflow now allows fast dataset recreation, detailed in the [Time and cost of writing the virtual dataset](#time-and-cost-of-writing-the-virtual-dataset) section. Performance testing shows time series extraction of 77M points in 36 seconds using Zarr-Python on a VEDA JupyterHub instance.

Future work includes incremental updates and batch rechunking to native Zarr.

## Introduction: Goals and Dataset Description

This document describes the structure of the NASA MUR SST dataset and the virtual [Icechunk](https://icechunk.io) dataset created from it. In the future, we plan to create a native Zarr version of this dataset as well, to optimized for time series generation.

### Goals:

- Share workflow for creating an icechunk virtual dataset from the MUR SST dataset.
- Expose and workaround issues in the underlying data files.
- Understand the performance of both writing and reading Icechunk stores.
- Compare virtual store read performance with reading from the original files.

### About the Dataset

Official Name: **GHRSST Level 4 MUR Global Foundation Sea Surface Temperature Analysis (v4.1) at PO.DAAC**

Official NASA Website: http://podaac.jpl.nasa.gov/Multi-scale_Ultra-high_Resolution_MUR-SST

S3 Bucket: `s3://podaac-ops-cumulus-protected/MUR-JPL-L4-GLOB-v4.1`

### Granules

There is one NetCDF4 file produced every day. The dataset begins on June 1st, 2002. As of March 10, 2025, the collection is comprised of 8,318 files.

### Internal File Structure

The files are standard NetCDF4 with 4 data variables across all files, and 2 additional variables which show up later in the dataset. Since variables need to be consistent across dimensions, these variables (`dt_1km_data`, `sst_anomaly`) are removed.

The other variables are listed below:

| name             | dtype   | shape             | original chunk shape | num. chunks  | 
| ---------------- | ------- | ----------------- | -------------------- | ----------- |
| time             | int32   | (1,)              | (1,)                 | 1           |
| lon              | float32 | (36000,)          | (36000,)             | 1           |
| lat              | float32 | (17999,)          | (17999,)             | 1           |
| analysed_sst     | int32   | (1, 17999, 36000) | (1, 1023, 2047)      | 324         |
| analysis_error   | float32 | (1, 17999, 36000) | (1, 1023, 2047)      | 324         |
| mask             | float32 | (1, 17999, 36000) | (1, 1447, 2895)      | 169         |
| sea_ice_fraction | float32 | (1, 17999, 36000) | (1, 1447, 2895)      | 169         |

The standard encodings are:

* `shuffle (elementsize=2), zlib (level=6)` for `analysed_sst` and `analysis_error`, and,
* `zlib (level=6), shuffle (elementsize=1)` for `mask` and `sea_ice_fraction`.

## Challenges and Solutions

### Inconsistencies in MUR SST Dataset and how we overcome them

The following tables describe the inconsistencies in the dataset, including **extra variables, encoding differences, and chunk shape changes** and how they are addressed.

#### Issues Summary

| **Issue Type**           | **Affected Time Periods**                                          | **Details**                                                                                                                               |
| ------------------------ | ------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------- |
| **Extra Variables**      |  Variables `dt_1km_data` and `sst_anomaly` are not in the original files  | Variables `dt_1km_data` and `sst_anomaly` are dropped because they are inconsistently present across all files.  |
| **Encoding Differences** | See [mursst_assessment.ipynb](../notebooks/mur-sst/mursst_assessment.ipynb) | Some files deviate from the standard encoding and must be written as native Zarr. |
| **Chunk Shape Changes**  | See [mursst_assessment.ipynb](../notebooks/mur-sst/mursst_assessment.ipynb) | Different chunk shapes appear during varied periods across variables. |


See [mursst_assessment.ipynb](../notebooks/mur-sst/mursst_assessment.ipynb) for a complete inventory of the variations in chunk shapes and codecs across variables.

For any time periods where the data files deviate from the original chunk shape OR the original encoding of that variable, data for those dates are written as native Zarr.

Because changes seem to be consistent again starting 2024-06-02, we will create a second virtual store starting on that date that can be appended to in an ongoing fashion.

---

## Implementation Approach

- [x] Establish a development environment for icechunk dataset generation.
- [x] [Complete initial virtual dataset](#writing-the-virtual-dataset).
- [x] Demonstrate how to read and [performance of the virtual dataset](#reading-from-and-performance-of-the-virtual-dataset).
- [x] Report on [time and cost to write virtual dataset](#time-and-cost-of-writing-the-virtual-dataset).

### Writing the virtual dataset

See [VirtualiZarr: Lithops Package for MUR SST Data Processing](https://github.com/zarr-developers/VirtualiZarr/tree/main/examples/mursst-icechunk-with-lithops). In this VirtualiZarr example, [lithops](https://lithops-cloud.github.io/) is used to parallelize generation of virtual and zarr datasets and writing those datasets to the icechunk store. Additionally, functions for generating a mean for a specific location over a given period, for both the icechunk store and using the original files, can be used for validation of the icechunk store.

### Time and cost of writing the virtual dataset

13 total hours of lambda runtime was used in the generation of this dataset. This includes periodic validation of the dataset. We can use the number of requests (9,124) and total time to estimate a dataset generation cost of [$1.23 using the AWS cost calculator](https://calculator.aws/#/estimate?id=fdddc3db021e70d7878acefb7579285eb16d2040). Storage cost, which includes some native zarr data, is estimated at [$4.44/year, again using the AWS cost calculator](https://calculator.aws/#/estimate?id=948cf887cd0fcdfa796e1e3cc5f72cc0facf9e4b).

### Reading from and performance of the virtual dataset

In [../notebooks/mur-sst/read_virtual.ipynb](../notebooks/mur-sst/read_virtual.ipynb), we demonstrate generating a 21 year time series of 1 square degree area in 36 seconds, using zarr-python. This equates to fetching 77,660,000 points!

Note, this test was run in us-west-2 using a VEDA JupyterHub instance with 60GB of memory and 15.7 vCPUs, and used a zarr-python runtime configuration for optimizing concurrency.

## Future work

- [ ] (IN PROGRESS) Create a virtual dataset from 2024-06-02 to present day.
- [ ] (IN PROGRESS) Incremental appending to the most recent virtual Icechunk dataset.
- [ ] Batch rechunking from virtual to native Zarr, stored in Icechunk.

### Additional virtual dataset from 2024-06-02 to present day

A current limitation of the Zarr specification and its implementations is array data must all have the same chunk shape, dimensions, and encodings. Ideally, the Zarr developer community would like to implement a variable array encoding solution in Zarr. This would allow arrays with different compression algorithms and chunk shapes to be accessed as a single array. See [Zarr extension for stacked / concatenated virtual views #288](https://github.com/zarr-developers/zarr-specs/issues/288) to learn more.

In lieu of a solution for concatenating arrays with different encodings and chunk shapes, we have written some periods as native zarr to the orignal Zarr store. Starting 2024-06-02 chunk shapes and encodings are consistent to present day, so this will constitute a new virtual dataset with new dates appended using CMR notifications.

However the ideal solution would be to enable concatenation of arrays with different encodings and chunk sizes. With that functionality in place, the existing store could be regenerated without any native Zarr data.

### Incremental appending to virtual Icechunk dataset

IN PROGRESS

### Batch rechunking from virtual to native Zarr, stored in Icechunk

TBD
