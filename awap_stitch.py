import argparse
import glob
import os
import yaml
from functools import partial

import xarray as xr
import numpy as np

from multiprocessing import Pool


def get_args():
    parser = argparse.ArgumentParser(description='Supply arguments to create subset of stitched netCDF files.')
    parser.add_argument("--gcm", required=True, help="Provide a model.")
    parser.add_argument("--rcp", required=True, help="Provide a rcp.")
    parser.add_argument("--start", required=True, help="Provide a start integer.", type=int)
    parser.add_argument("--end", required=True, help="Provide end integer", type=int)
    parser.add_argument("--input_base_dir", required=True, help"The base directory where to find input .nc files. The subfolder structure is assumed.")
    parser.add_argument("--output_base_dir", required=True, help"The base directory where to write output. Files will be written to subfolders based on model, rcp, etc.")
    
    args = parser.parse_args()
    return args


def pre_stitch(args, i):
    if i == 0:
        l1 = glob.glob(f'{args.input_base_dir}/{args.gcm}/{args.rcp}/{i}_*.nc')
        l2 = glob.glob(f'{args.input_base_dir}/{args.gcm}/{args.rcp}/{i + 1}_*.nc')
        l3 = glob.glob(f'{args.input_base_dir}/{args.gcm}/{args.rcp}/{i + 2}_*.nc')
        flist = l1 + l2 + l3
        
    elif i == 280:
        l1 = glob.glob(f'{args.input_base_dir}/{args.gcm}/{args.rcp}/{i*3}_*.nc')
        flist = l1

    else:
        l1 = glob.glob(f'{args.input_base_dir}/{args.gcm}/{args.rcp}/{i*3}_*.nc')
        l2 = glob.glob(f'{args.input_base_dir}/{args.gcm}/{args.rcp}/{i*3 + 1}_*.nc')
        l3 = glob.glob(f'{args.input_base_dir}/{args.gcm}/{args.rcp}/{i*3 + 2}_*.nc')
        flist = l1 + l2 + l3

    print('number', i)
    ds = xr.open_mfdataset(flist, chunks={'lat':5, 'lon':841}, combine='by_coords')
    ds.time.attrs['bounds'] = 'time_bnds'
    ds.time.encoding['dtype'] = np.dtype('double')
    ds = ds.transpose('time', 'lat', 'lon')
    ds = ds.chunk(chunks={'time':100,'lat':681,'lon':841})
    comp = dict(dtype="float32", zlib=True, complevel=9)
    encoding = {var: comp for var in ds.data_vars}
    
    outdir = os.path.join(
        args.output_base_dir,
        args.gcm,
        args.rcp)
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    outfile = os.path.join(
        outdir,
        f'{i}_{args.gcm}_{args.rcp}.nc4')
    ds.to_netcdf(outfile, mode='w', encoding=encoding, unlimited_dims=['time'])

    print(f'Part {i} completed')


if __name__ == "__main__":
    args = get_args()
    print(f'Running range {args.start}, {args.end}')

    # Wrap the pre_stitch() function we want to use for multiprocessing
    # in a partial func, so that we can pass additional args to it.
    pre_stitched_wrapped_function = partial(pre_stitch, args)

    with Pool() as p:
        p.map(pre_stitched_wrapped_function, range(args.start, args.end))