import argparse
import concurrent.futures
import glob
import os
import yaml

import pandas as pd
import xarray as xr
import numpy as np


def get_config():
    """Return config file as an object"""
    try:
        stream = open(f'{os.path.dirname(__file__)}/config.yaml', 'r')
        stream = yaml.safe_load(stream)
        return stream
    except:
        print('ERROR: Config file failed to open!')
        exit()

def get_args():
    parser = argparse.ArgumentParser(description='Supply arguments to stitch ".dat" files into netCDF files.')
    parser.add_argument("model_id", help="Provide a model.")
    parser.add_argument("rcp", help="Provide the RCP")
    parser.add_argument("time_period", help="Provide a time span.")

    args = parser.parse_args()
    return args


def generate_working_dir(args, cfg):
    '''
    Build the filepath for the working directory as specified in the configuration file if they do not exist
    '''
    if not os.path.exists(cfg['working_dir']):
        os.makedirs(cfg['working_dir'])

    if (args.time_period == 'gcmc'):
        outpath = f'{cfg["working_dir"]}/bias_corrected/{args.model_id}/{args.time_period}'
    else:
        outpath = f'{cfg["working_dir"]}/bias_corrected/{args.model_id}/{args.time_period}_{args.rcp}'

    if not os.path.exists(outpath):
        os.makedirs(outpath)


def get_files(args, cfg):
    try:
        if (args.rcp == 'historical'):
            # Line below will probably need to be updated...
            file_list = glob.glob(f'{cfg["source_data"]}/{args.model_id}/rcp45/*/{args.time_period}*dat')
        else:
            file_list = glob.glob(f'{cfg["source_data"]}/{args.model_id}/{args.rcp}/*/{args.time_period}*dat')
    except:
        print('ERROR: Bad file path!')
    return file_list


def get_lat_lon_step(df, gcm, cfg):
    '''
    Using the underscore as a delimiter, use the files name as an input with the first value indicating lon and the following value indicating lat.
    '''
    filename, ext = os.path.basename(df).split('.')
    file_attributes = [x for x in filename.split("_")]
    _, _, ilon, ilat = file_attributes
    ilon = float (ilon)
    ilat = float (ilat)

    initial = Coord(lon = cfg[gcm]['initial_lon'], lat = cfg[gcm]['initial_lat'])
    step = Coord(lon=cfg[gcm]['step_lon'], lat=cfg[gcm]['step_lat'])

    nstep = Coord(lon=ilon, lat=ilat)
    offset = initial.offset(step, nstep)

    return offset.lon, offset.lat, ilon, ilat


def get_gcm(dictionary):
    '''
    Return value of selected GCM dictionary
    '''
    for key, val in dictionary.items():
        if val:
            return key
    raise SystemExit("ERROR: Script quitting!\n::No GCM value set to TRUE in the config!")


def get_dataframe(file):
    '''
    Open each file as a Dataframe in pandas.
    Apply a label to each column.
    Concatenate the 'year'/'month'/'day' variables into a single 'time' variable.
    Delete the old 'year'/'month'/'day' variables.
    '''
    df = pd.DataFrame(np.loadtxt(file))
    df.columns = ['year', 'month', 'day', 'pr', 'tasmax', 'tasmin', 'sfcWind', 'rsds']
    df['time'] = df.apply(lambda row: pd.Timestamp(year=int(row[0]), month=int(row[1]), day=int(row[2])), axis=1)
    ordered_df = df.drop(['year', 'month', 'day'], axis=1)
    return ordered_df


def resolve_lat_lon(df, lon, lat):
    '''
    Apply the lon and lat values to the dataframe.
    '''
    df['lon'] = lon
    df['lat'] = lat
    new_df = df.set_index(['lat', 'lon', 'time'])
    return new_df

def multiprocess_files(file):
    cfg = get_config()
    args = get_args()
    df = get_dataframe(file)
    lon, lat, ilon, ilat = get_lat_lon_step(file, args.model_id, cfg)
    df2ds = resolve_lat_lon(df, lon, lat)
    ds = df2ds.to_xarray()

    if (args.time_period == 'gcmc'):
        outpath = f'{cfg["working_dir"]}/bias_corrected/{args.model_id}/{args.time_period}'
    else:
        outpath = f'{cfg["working_dir"]}/bias_corrected/{args.model_id}/{args.time_period}_{args.rcp}'

    ds.to_netcdf(path=f"{outpath}/{int(ilon)}_{int(ilat)}.nc", mode='w', engine='netcdf4')


def stitch_ncfiles(args, cfg):
    file_paths = glob.glob(f'{cfg["working_dir"]}/bias_corrected/{args.model_id}/{args.time_period}_{args.rcp}/*')
    ds = xr.open_mfdataset(file_paths, combine='by_coords')
    ds.time.attrs['bounds'] = 'time_bnds'
    ds.time.encoding['dtype'] = np.dtype('double')
    ds = ds.transpose('time', 'lat', 'lon')

    if not os.path.exists(f'{cfg["working_dir"]}/bias_corrected_stitched/{args.model_id}/'):
        os.makedirs(f'{cfg["working_dir"]}/bias_corrected_stitched/{args.model_id}/')

    ds.to_netcdf(f'{cfg["working_dir"]}/bias_corrected_stitched/{args.model_id}/{args.model_id}_mrnbc-{args.rcp}_stitched_var.nc4')
    for var in ds.data_vars.values():
        var.to_netcdf(f'{cfg["working_dir"]}/bias_corrected_stitched/{args.model_id}/{var.name}_{args.model_id}_{args.rcp}_mrnbc_stitched.nc4', unlimited_dims=['time'])


def subdivide_cdf(ds, lat_bnds, lon_bnds, path):
    new_ds = subset.subset_bbox(ds,lat_bnds=lat_bnds,lon_bnds=lon_bnds)
    new_ds.to_netcdf(f'{path}.nc4')


def sda(args, cfg):
    vars = ['pr', 'tasmin', 'tasmax', 'sfcWind', 'rsds']
    for i in vars:
        ds = xr.open_dataset(f'{cfg["working_dir"]}/bias_corrected_stitched/{args.model_id}/{i}_{args.model_id}_{args.rcp}_mrnbc_stitched.nc4')
        
        if not os.path.exists(f'{cfg["working_dir"]}/sda_prep/{args.model_id}/{i}_mrnbc_{args.rcp}/'):
            os.makedirs(f'{cfg["working_dir"]}/sda_prep/{args.model_id}/{i}_mrnbc_{args.rcp}/')

        for lats in reversed(range(1, ds.dims['lat']-1)):
                for lons in range(1, ds.dims['lon'] - 1):
                    lonvals = [float(ds['lon'][lons -1]), float(ds['lon'][lons +1])]
                    latvals = [float(ds['lat'][lats -1]), float(ds['lat'][lats +1])]
                    subdivide_cdf(ds, latvals, lonvals, f'{cfg["working_dir"]}/sda_prep/{args.model_id}/{i}_mrnbc_{args.rcp}/{lons}_{(ds.dims["lat"] -1) - lats}')

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


if __name__ == "__main__":
    cfg = get_config()
    args = get_args()
    generate_working_dir(args, cfg)
    print(f'Running for: {args.model_id}, {args.rcp}, {args.time_period}')

    with concurrent.futures.ProcessPoolExecutor() as executor:
        dat_files = get_files(args, cfg)
        for open_file, _ in zip(dat_files, executor.map(multiprocess_files, dat_files)):
            continue
        print ("File splitting COMPLETE. \nStitching .nc files together...")
    
    stitch_ncfiles(args, cfg)
    print("File stitching COMPLETE! \nBeginning SDA preparation...")

    sda_prep(args, cfg)
    print("SDA prearation complete!")