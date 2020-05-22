import argparse
import concurrent.futures
import glob
import numpy as np
import os
import pandas as pd
import xarray as xr

global_rcp = 'rcp85'
def input_check(args, list, exitstring):
    if (args not in list):
        return exitstring

def parse_input(): # Check input against pre-defined lists.

    model_list = ('CNRM-CM5', 'GFDL-ESM2M', 'ACCESS1-0', 'MIROC5')
    time_list = ('bc_cur', 'bc_fut', 'gcmc', 'gcmf')

    parser = argparse.ArgumentParser() # Read the command line, setting reasonable default values.
    parser.add_argument("input_dir", help="Provide base directory path that houses the GCM directories")
    parser.add_argument("model_id", choices=model_list, help="Provide a model.")
    parser.add_argument("t_period", choices=time_list, help="Provide a time span.")
    args = parser.parse_args()

    input_check(args.model_id, model_list, "Invalid input. Entry not found")
    input_check(args.t_period, time_list, "Invalid input. Entry not found")

    return args.input_dir, args.model_id.upper(), args.t_period

input_dir, model_dir, t_period = parse_input()
if input_dir.endswith('/'):
    input_dir = input_dir[:-1]

def discover_files(input_dir, model_dir, t_period):
    bc_datasets = glob.glob(f'{input_dir}/{model_dir}/{global_rcp}/*/{t_period}*dat')
    return bc_datasets

def open_dataframe(open_file):
    '''
    Open each file as a Dataframe in pandas.
    Apply a label to each column.
    Concatenate the 'year'/'month'/'day' variables into a single 'time' variable.
    Delete the old 'year'/'month'/'day' variables.
    '''
    load_df = pd.DataFrame(np.loadtxt(open_file))
    load_df.columns = ['year', 'month', 'day', 'pr', 'tasmax', 'tasmin', 'sfcWind', 'rsds']
    load_df['time'] = load_df.apply(lambda row: pd.Timestamp(year=int(row[0]), month=int(row[1]), day=int(row[2])), axis=1)
    ordered_df = load_df.drop(['year', 'month', 'day'], axis=1)
    return ordered_df

def find_lon_lat_step(open_df):
    '''
    Using the underscore as a delimiter, use the files name as an input with the first value indicating lon and the following value indicating lat.
    '''
    filename, ext = os.path.basename(open_df).split('.')
    file_attributes = [x for x in filename.split("_")]
    _, _, ilon, ilat = file_attributes
    ilon = float (ilon)
    ilat = float (ilat)
    return ilon, ilat

class Coord:
    '''
    Class which allows us to create an object that has a base lon and lat.
    Which can then be increased by predetermined increments using an integer (in this case the filename) to determine the number of steps to create a final result.
    '''
    def __init__(self, lon, lat):
        self.lon = lon
        self.lat = lat

    def offset(self, step_coord, nstep_coord):
        return self + step_coord * nstep_coord

    def __add__(self, other):
        return Coord(lon=self.lon + other.lon, lat=self.lat + other.lat)

    def __mul__(self, other):
        return Coord(lon=self.lon * other.lon, lat=self.lat * other.lat)

def coords_from_path(ilon, ilat):
    '''
    Using Coord class which allows us to create an object that has a base lon and lat.
    Which can then be increased by predetermined increments using an integer (in this case the filename) to determine the number of steps to create a final result.
    '''
    ### ACCESS1-0 ###
    # initial = Coord(lon = 108.75, lat = -6.25)
    # step = Coord(lon=1.875, lat=-1.25)

    ### CNRM-CM5 ###
    # initial = Coord(lon = 108.28125, lat = -6.303452187571035)
    # step = Coord(lon=1.40625, lat=-1.40076385695359)
    ### MIROC5 ###
    initial = Coord(lon = 108.75, lat = -7.07865168539325)
    step = Coord(lon=2.5, lat=-2.02247191011236)
    ### GFDL-ESM2M ###
    # initial = Coord(lon = 108.28125, lat = -6.303592215351715)
    # step = Coord(lon=1.40625, lat=-1.40076386617093)

    nstep = Coord(lon=ilon, lat=ilat)
    offset = initial.offset(step, nstep)
    # print(f"{offset.lon} {offset.lat}")
    return offset.lon, offset.lat

def fix_the_indicies(ordered_df, lon, lat):
    '''
    Apply the lon and lat values to the dataframe.
    '''
    ordered_df['lon'] = lon
    ordered_df['lat'] = lat
    primed_df = ordered_df.set_index(['lat', 'lon', 'time'])
    return primed_df

def process_file(open_file):
    '''
    Convert from Pandas dataframe to xarray dataset. Save to output folder, using model_dir and t_period set at the start as directory path. 
    '''
    ordered_df = open_dataframe(open_file)
    ilon, ilat = find_lon_lat_step(open_file)
    lon, lat = coords_from_path(ilon, ilat)
    df2xr = fix_the_indicies(ordered_df, lon, lat)
    xr_dataset = df2xr.to_xarray()
    outpath = f'final_outputs/bc/{model_dir}/{t_period}'
    # outpath = f'final_outputs/bc/{model_dir}/{t_period}_{global_rcp}'
    if not os.path.exists(outpath):
        os.makedirs(outpath)
    xr_dataset.to_netcdf(path=f"{outpath}/{int(ilon)}_{int(ilat)}.nc", mode='w', engine='netcdf4')

# def stitch_ncfiles():
#     # Glob the output to load in multiple files
#     # file_paths = glob.glob(f'outputs/{model_dir}/{t_period}/*')
#     gcmlist = ['CNRM-CM5', 'ACCESS1-0', 'MIROC5', 'GFDL-ESM2M']
#     bcrcp = ['rcp45']
#     for j in gcmlist:
#         for z in bcrcp:
#             k = 'rcp45'
#             file_paths = glob.glob(f'/g/data/er4/jr6311/unsw-bias-correction/final_outputs/bc/{j}/bc_fut/*')

#             # Load up files
#             ds = xr.open_mfdataset(file_paths, combine='by_coords')
#             ds.to_netcdf(f'final_outputs/bc_stitched/{j}/{j}_unsw-{k}_stitched_var.nc4')

#             ds = xr.open_dataset(f'final_outputs/bc_stitched/{j}/{j}_unsw-{k}_stitched_var.nc4')
#             # ds = ds.expand_dims({'bnds': 2})
#             ds.time.attrs['bounds'] = 'time_bnds'
#             ds.time.encoding['dtype'] = np.dtype('double')
#             ds = ds.transpose('time', 'lat', 'lon')
#             ds.to_netcdf(f'final_outputs/bc_stitched/{j}/{j}_unsw-{k}_stitched_allvars.nc4', unlimited_dims=['time'])

#             # for data_var in ds.data_vars.values():
#             #     data_var.to_netcdf(f'{data_var.name}_{model_dir}_{t_period}.nc4', unlimited_dims='time')
#             for data_var in ds.data_vars.values():
#                 data_var.to_netcdf(f'final_outputs/bc_stitched/{j}/{data_var.name}_{j}_unsw-{k}_stitched.nc4', unlimited_dims=['time'])

if __name__ == "__main__":
    with concurrent.futures.ProcessPoolExecutor() as executor:
        dat_files = discover_files(input_dir, model_dir, t_period)
        for open_file, _ in zip(dat_files, executor.map(process_file, dat_files)):
            continue
        print ("File splitting COMPLETE. \nStitching .nc files together...")
    # stitch_ncfiles()
    # print("File stitching COMPLETE!")