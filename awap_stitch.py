import argparse
import glob
import os
import yaml

import xarray as xr
import numpy as np

from multiprocessing import Pool


def get_args():
    parser = argparse.ArgumentParser(description='Supply arguments to create subset of stitched netCDF files.')
    parser.add_argument("gcm", help="Provide a model.")
    parser.add_argument("rcp", help="Provide a rcp.")
    parser.add_argument("start", help="Provide a start integer.", type=int)
    parser.add_argument("end", help="Provide end integer", type=int)
    
    args = parser.parse_args()
    return args


def get_config():
    """Return config file as an object"""
    try:
        stream = open(f'{os.path.dirname(__file__)}/config.yaml', 'r')
        stream = yaml.safe_load(stream)
        return stream
    except:
        print('ERROR: Config file failed to open!')
        exit()


def pre_stitch(i):
    args = get_args()
    cfg = get_config()

    if i == 0:
        l1 = glob.glob(f'/scratch/er4/jr6311/final_mrnbc/awap/bias_corrected/{gcm}/{rcp}/{i}_*.nc')
        l2 = glob.glob(f'/scratch/er4/jr6311/final_mrnbc/awap/bias_corrected/{gcm}/{rcp}/{i + 1}_*.nc')
        l3 = glob.glob(f'/scratch/er4/jr6311/final_mrnbc/awap/bias_corrected/{gcm}/{rcp}/{i + 2}_*.nc')
        flist = l1 + l2 + l3
        
    elif i == 280:
        l1 = glob.glob(f'/scratch/er4/jr6311/final_mrnbc/awap/bias_corrected/{gcm}/{rcp}/{i*3}_*.nc')
        flist = l1

    else:
        l1 = glob.glob(f'/scratch/er4/jr6311/final_mrnbc/awap/bias_corrected/{gcm}/{rcp}/{i*3}_*.nc')
        l2 = glob.glob(f'/scratch/er4/jr6311/final_mrnbc/awap/bias_corrected/{gcm}/{rcp}/{i*3 + 1}_*.nc')
        l3 = glob.glob(f'/scratch/er4/jr6311/final_mrnbc/awap/bias_corrected/{gcm}/{rcp}/{i*3 + 2}_*.nc')
        flist = l1 + l2 + l3

    print('number', i)
    ds = xr.open_mfdataset(flist, chunks={'lat':10, 'lon':10}, combine='by_coords')
    ds.time.attrs['bounds'] = 'time_bnds'
    ds.time.encoding['dtype'] = np.dtype('double')
    ds = ds.transpose('time', 'lat', 'lon')
    ds = ds.chunk(chunks={'time':100,'lat':681,'lon':841})
    comp = dict(zlib=True, complevel=9)
    encoding = {var: comp for var in ds.data_vars}
    

    if not os.path.exists(f'{cfg["working_dir"]}/awap/bias_corrected_subset/{args.gcm}/{args.rcp}/'):
        os.makedirs(f'{cfg["working_dir"]}/awap/bias_corrected_subset/{args.gcm}/{args.rcp}/')

    ds.to_netcdf(f'{cfg["working_dir"]}/awap/bias_corrected_subset/{args.gcm}/{args.rcp}/{i}_{args.gcm}_{args.rcp}_subset_mrnbc.nc4', mode='w', encoding=encoding, unlimited_dims=['time'])

    print(f'Part {i} completed')


if __name__ == "__main__":
    args = get_args()
    print(f'Running range {args.start}, {args.end}')
    with Pool() as p:
        p.map(pre_stitch, range(args.start, args.end))