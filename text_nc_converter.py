import argparse
import concurrent.futures
import glob
import os
import yaml
from functools import partial

import pandas as pd
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
    parser.add_argument("--model_id", required=True, help="Provide a model.")
    parser.add_argument("--rcp", required=True, help="Provide the RCP")
    parser.add_argument("--time_period", required=True, help="Provide a time span.")
    parser.add_argument("--scale", required=True, choices=['gcm', 'awap'], help="Specify whether processing for GCM or AWAP scale data.")
    parser.add_argument("--input_base_dir", required=True, help"The base directory where to find input .dat files. The subfolder structure is assumed.")
    parser.add_argument("--output_base_dir", required=True, help"The base directory where to write output. Files will be written to subfolders based on model, rcp, etc.")
    
    args = parser.parse_args()
    return args


def get_output_dir(args):
    return os.path.join(
        args.output_base_dir,
        args.model_id,
        args.rcp)


def create_output_dir(args):
    '''
    Create output dir if it does not exist
    '''
    outpath = get_output_dir(args)
    if not os.path.exists(outpath):
        os.makedirs(outpath)


def get_files(args):
    try:
        file_list = glob.glob(f'{args.input_base_dir}/{args.model_id}/{args.rcp}/**/{args.time_period}*dat', recursive=True)
    except:
        print('ERROR: Bad file path!')
    return file_list


def get_lat_lon_step(df, gcm, cfg, scale):
    '''
    Using the underscore as a delimiter, use the files name as an input with the first value indicating lon and the following value indicating lat.
    '''
    filename, ext = os.path.basename(df).split('.')
    file_attributes = [x for x in filename.split("_")]
    _, _, ilon, ilat = file_attributes
    ilon = float (ilon)
    ilat = float (ilat)

    if (scale == 'awap'):
        ckey = cfg['AWAP']
    else:
        ckey = cfg[gcm]

    initial = Coord(lon = ckey['initial_lon'], lat = ckey['initial_lat'])
    step = Coord(lon=ckey['step_lon'], lat=ckey['step_lat'])

    nstep = Coord(lon=ilon, lat=ilat)
    offset = initial.offset(step, nstep)

    return offset.lon, offset.lat, ilon, ilat


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


def process_file(cfg, args, file):
    '''
    Function to process a single file
    '''
    df = get_dataframe(file)
    lon, lat, ilon, ilat = get_lat_lon_step(file, args.model_id, cfg, args.scale)

    outpath = get_output_dir(args)

    if not os.path.exists(f'{outpath}/{int(ilon)}_{int(ilat)}.nc'):
        df2ds = resolve_lat_lon(df, lon, lat)
        ds = df2ds.to_xarray()
        comp = dict(zlib=True, complevel=9)
        encoding = {var: comp for var in ds.data_vars}
        ds['pr'] /= 86400
        ds['rsds'] /= (86400/1000000)
        ds['tasmax'] += 273.15
        ds['tasmin'] += 273.15
        ds.to_netcdf(path=f"{outpath}/{int(ilon)}_{int(ilat)}.nc", mode='w', encoding=encoding, engine='netcdf4')


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
    create_output_dir(args)
    print(f'Running for: {args.model_id}, {args.rcp}, {args.time_period}, SCALE:{args.scale}')

    # In order to use executor.map to apply the process_file() function for
    # each file, whilst also taking more than one argument (i.e. cfg and args),
    # need to wrap in "partial" function. Lambda should also work, but for some
    # reason, get an error that lambda is not picklable, but partial function works.
    process_file_wrapped_function = partial(process_file, cfg, args)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        dat_files = get_files(args)
        for open_file, _ in zip(dat_files, executor.map(process_file_wrapped_function, dat_files)):
            continue
    
    print ("Text to .nc conversion complete.")
