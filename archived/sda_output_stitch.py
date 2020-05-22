import argparse
import xarray as xr
import glob

def get_args():
    parser = argparse.ArgumentParser(description='Supply arguments to stitch netCDF files together')
    parser.add_argument('input_filepath', help='The filepath of stored nc files')
    parser.add_argument('variable', help='Provide a variable for which to combine the dataset over')
    parser.add_argument('gcm', help='Provide the GCM of the dataset')
    parser.add_argument('rcp', help='Provide the rcp of the dataset')
    parser.add_argument('output_filepath', help='Provide a filepath for outputs')

    args = parser.parse_args()
    return args

def get_year(rcp):
    if rcp == 'historical':
        return '1960-2005'
    elif rcp == 'rcp45' or rcp == 'rcp85' :
        return '2006-2099'
    else:
        raise NameError(rcp)

if __name__ == "__main__":
    args = get_args()
    filepath = f'{args.input_filepath}/{args.gcm}/{args.rcp}/{args.variable}/*'
    filelist = glob.glob(filepath)
    year = get_year(args.rcp)

    ds = xr.open_mfdataset(filelist, combine='by_coords')
    ds.to_netcdf(f'{args.output_filepath}/{args.variable}_day_{args.rcp}_{args.gcm}_{year}.nc4', unlimited_dims=['time'])
