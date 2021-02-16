import argparse
import xarray as xr
import dask as da
import glob
import os

from dask.distributed import Client


def get_args():
    parser = argparse.ArgumentParser(description='Supply arguments to create subset of stitched netCDF files.')
    parser.add_argument("--gcm", required=True, help="Specify the model. e.g. ACCESS1-0")
    parser.add_argument("--rcp", required=True, help="Specify the scenario. e.g. rcp45")
    parser.add_argument("--input_base_dir", required=True, help="The base directory containing files to stitch. Subfolder for gcm and rcp is assumed.")
    parser.add_argument("--output_base_dir", required=True, help="The base directory to write stitched files to.")
    parser.add_argument("--dask_temp_dir", required=True, help="The directory for dask to hold temporary files")

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = get_args()

    da.config.set({'temporary_directory': args.dask_temp_dir})
    client = Client(n_workers=8)
    print(client)

    flist = glob.glob(f'{args.input_base_dir}/{args.gcm}/{args.rcp}/*.nc*')
    ds = xr.open_mfdataset(flist, chunks={'lat':681, 'lon':841}, parallel=True, combine='by_coords')
    ds = ds.chunk(chunks={'time':1000,'lat':681,'lon':841})
    for var in ds.data_vars.values():
        comp = dict(dtype="float32", zlib=False, complevel=9)
        encoding = {var.name: comp}

        outdir = os.path.join(
            args.output_base_dir,
            args.gcm,
            args.rcp)
        if not os.path.exists(outdir):
            os.makedirs(outdir)

        outfile = os.path.join(
            outdir,
            f'{var.name}_mrnbc_{args.gcm}_{args.rcp}.nc4')

        var.to_netcdf(outfile, mode='w', engine='netcdf4', encoding=encoding, unlimited_dims=['time'])
        print(f'{var.name} has finished processing')
