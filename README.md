# MRNBC Stitching
This repo contains a set of scripts and jobs for "stitching" the output of MRNBC.



# Background
MRNBC is a bias correction method. For each model (e.g. ACCESS1-0) and RCP (e.g. rcp45), it outputs a text file per grid cell (.dat). This text file contains daily data for multiple variables at that grid cell.

The amount of timesteps (i.e. lines) in one of these text files can be the whole RCP range (e.g. 2006-01-01 to 2099-12-31), or just a time slice (e.g. 2036-01-01 to 2065-12-31); it depends on how MRNBC was run. Most recently, it is run with separate time slices.

The .dat files use a particular naming convention: `{period}_{lonIndex}_{latIndex}.dat`.
For example, `bc_fut_350_12.dat`.

`period` can be one of two values, `bc_fut` for rcp45/rcp85, or `bc_cur` for historical.

`lonIndex` and `latIndex` in the filename refer to lon/lat indices, relative to the starting lat/lon. For HydroProjections, there are 841 longitudes, and 681 latitudes. The starting lon is 112.00, and the starting lat is -10.00. Grid resolution is 5km (or 0.05). Indices advance such that:
* lon index 0 = 112.00
* lon index 840 = 154.00
* lat index 0 = -10.00
* lat index 680 = -44.00

That is, indices advance from "top left to bottom right".

MRNBC is run such that the **historical** period (`bc_cur`) is exported at the same time as **rcp85**, and thus is written to the same folder containing `bc_fut` files. In case of multiple time slices, it is output to the folder for the first time slice (i.e. 2006-2035).

The MRNBC stitching scripts perform two main functions: convert the text files into NetCDF files, and to stitch the files for individual grids into a single file. This is accomplished via a sequence of PBS jobs.



# Job Sequence
1. `stitch_text_time_slices`
   * Use this to stitch .dat files for the same model/RCP across multiple timeslices
   * The time slices are 2006-2035, 2036-2065, 2066-2095, 2070-2099
   * This is needed for rcp45 and rcp85, and **not** historical
     * Historical files already contain all time steps, and do not need to be combined
   * Modify `stitch_text_time_slices.sh` for the desired model/RCP, and run the shell script
     * It will create PBS jobs from a template file and submit them
   * This job is done first to reduce total number of (temporary) files produced. Otherwise, subsequent steps would also need to be done per time slice, which can consume a lot of inode quota.
2. `convert_text_to_nc`
   * This will take a .dat file, and convert it to NetCDF file
   * It also performs unit conversion
   * Modify `convert_text_to_nc.pbs` for the desired model/RCP, then submit the job
3. `stitch_cells_into_lons`
   * This will take individual .nc files, and merge them into longitudinal strips of 3 lons per strip
   * This job is an intermediate stitching step to help alleviate memory/processing issues
   * Modify `stitch_cells_into_lons.sh` for the desired model/RCP, then run the shell script
     * It will create PBS jobs from a template file and submit them
4. `stitch_final`
   * This will take .nc files in a folder, and merge them into a single .nc file
   * The .nc files up until this point contain multiple variables
   * This job will output one .nc file per variable for the model/RCP
   * Modify `stitch_final.pbs` for the desired model/RCP, then submit the job



# After Stitching
After `stitch_final`, there are further postprocessing steps to perform:
* Despeckling
* Transform Winds
* Compliance Checker
The scripts/jobs for these are located in other repos

## Despeckling
MRNBC outputs for pr and tasmin for rcp45/rcp85 exhibit "speckling"; some small percentage of grid cells (spread out over the entire domain with no discernable pattern) are set at the maximum allowable MRNBC output value. This is likely an artifact of how MRNBC is being compiled or run.

Despeckling removes these cells and fills them back in with interpolated values from surrounding cells.

The scripts can be found in `mrnbc-despeckle` repo.

## Despeckling Also Fills In Missing Cells
In addition to "speckled" cells, there are some percentage of missing cells. These occur outside of land for the most part, and a small number of missing cells are over land.

Unlike "speckled" cells however, missing cells appear to affect all variables and RCPs to varying degrees.

The despeckling process, due to how it works, also fills in these missing cells. 

Therefore, the despeckling PBS job can be run on all MRNBC stitched files.

## Wind Transform
`sfcWind` is at a height at 10m when output from MRNBC. To convert from 10m to 2m, use the scripts found in `transform-wind-grids` repo.

An important thing to note is that **these scripts were not made specifically for MRNBC or even Hydro Projections**, and there is no concept of a GCM or RCP. Therefore, the PBS jobs need more inputs specified than most other scripts and PBS jobs designed for Hydro Projections. In particular, pay attention to the start and end years, and input/output paths.

There are some further complications with how the Wind Transform scripts work:
* Assumes data is indexed such that latitude is oriented North to South
  * But the stitching process produces data that is oriented South to North
* Wind Transform script uses the variable name as the output filename regardless of the input filename
* Wind Transform script outputs one file per year, regardless of how many years are in input file
  * This is not necessarily a problem in and of itself, but the next step after Wind Transform, Compliance Checker for MRNBC, has not been setup to handle multiple input files and only deals with one input file per job

This means that to Wind Transform for a single MRNBC sfcWind file, 3 PBS jobs need to be run:
* Invert latitude
* Run actual Wind Transform
* Merge yearly outputs back into a single file

## Compliance Checker
The final step is Compliance Checker, which will standardise the metadata. The Compliance Checker for MRNBC also applies the AWAP mask.

The scripts can be found in `compliance_checking` repo.

Compliance Checker is a two part process:
* Run `run_cc_hydroproj_mrnbc.sh`
  * Submits multiple jobs, one per GCM/RCP/variable combo
* Run `run_merge_hydroproj.sh`
  * Submits multiple jobs, one per GCM/RCP/variable combo

In both cases, check that the variables named `variables`, `rcps`, and `gcm` are properly set for MRNBC and which variables/RCPs to run for. Additionally for `run_merge_hydroproj.sh`, also check that `bias_correction_method` is set to `MRNBC`.

By default, these scripts are setup to run for all variables and RCPs for a given model.