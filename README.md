# MRNBC Stitching
This repo contains a set of scripts and jobs for "stitching" the output of MRNBC.



# Background
MRNBC is a bias correction method. For each model (e.g. ACCESS1-0) and RCP (e.g. rcp45), it outputs a text file per grid cell (.dat). This text file contains daily data for multiple variables at that grid cell.

The amount of timesteps (i.e. lines) in one of these text files can be the whole RCP range (e.g. 2006-01-01 to 2099-12-31), or just a time slice (e.g. 2036-01-01 to 2065-12-31).

The .dat files use a particular naming convention: `{period}_{lonIndex}_{latIndex}.dat`.
For example, `bc_fut_350_12.dat`.

`period` can be one of two values, `bc_fut` for RCP45/RCP85, or `bc_cur` for historical.

`lonIndex` and `latIndex` in the filename refer to lon/lat indicies, relative to the starting lat/lon. For HydroProjections, there are 841 longitudes, and 681 latitudes. The starting lon is 112.00, and the starting lat is -10.00. Grid resolution is 5km (or 0.05). Indicies advance such that:
* lon index 0 = 112.00
* lon index 840 = 154.00
* lat index 0 = -10.00
* lat index 680 = -44.00
That is, indicies advance from "top left to bottom right".

MRNBC is run such that the historical period (`bc_cur`) is exported at the same time as rcp85, and thus is written to the same folder containing `bc_fut` files.

The MRNBC stitching scripts perform two main functions: convert the text files into NetCDF files, and to stitch the files for individual grids into a single file.



# Job Sequence
1. `stitch_text_time_slices`
   * Use this to stitch .dat files for the same model/rcp across multiple timeslices
   * The time slices are 2006-2035, 2036-2065, 2066-2095, 2070-2099
   * This is needed for rcp45 and rcp85, and **not** historical
     * Historical files contain all time steps, and do not need to be combined
   * Modify `stitch_text_time_slices.sh` for the desired model/rcp, and run the shell script
     * It will create PBS jobs from a template file and submit them
2. `convert_text_to_nc`
   * This will take a .dat file, and convert it to NetCDF file
   * Modify `convert_text_to_nc.pbs` for the desired model/rcp, then submit the job
3. `stitch_cells_into_lons`
   * This will take individual .nc files, and merge them into longitudinal strips of 3 lons per strip
   * This job is an intermediate stitching step to help alleviate memory/processing issues
   * Modify `stitch_cells_into_lons.sh` for the desired model/rcp, then run the shell script
     * It will create PBS jobs from a template file and submit them
4. `stitch_final`
   * This will take .nc files in a folder, and merge them into a single .nc file
   * The .nc files up until this point contain multiple variables
   * This job will output one .nc file per variable for the model/rcp
   * Modify `stitch_final.pbs` for the desired model/rcp, then submit the job
