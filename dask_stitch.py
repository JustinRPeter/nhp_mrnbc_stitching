import xarray as xr
import dask as da
import glob

from dask.distributed import Client


# def get_config():
#     """Return config file as an object"""
#     try:
#         stream = open(f'{os.path.dirname(__file__)}/config.yaml', 'r')
#         stream = yaml.safe_load(stream)
#         return stream
#     except:
#         print('ERROR: Config file failed to open!')
#         exit()


# def get_args():
#     parser = argparse.ArgumentParser(description='Supply arguments to stitch ".dat" files into netCDF files.')
#     parser.add_argument("gcm", help="Provide a model.")
#     parser.add_argument("rcp", help="Provide the RCP")
    
#     args = parser.parse_args()
#     return args


if __name__ == "__main__":
        # cfg = get_config()
        # args = get_args()    
        
        da.config.config
        da.config.set({'temporary-directory': '/scratch/er4/jr6311/dask_temp'})
        da.config.set({'dataframe':{'shuffle-compression': 'Zlib'}})
        client = Client(n_workers=20)

        print(client)
        
        flist = glob.glob(f'/g/data/er4/jr6311/mrnbc/stitch_ACCESS1-0_rcp85/*')
        # file_list = cfg["working_dir"]}/awap/bias_corrected_subset/{args.gcm}/{args.rcp}/
        # ds = xr.open_mfdataset(file_list, chunks={'lat':10, 'lon':10}, combine='by_coords')
        ds = xr.open_mfdataset(flist, chunks={'lat':10, 'lon':10}, combine='by_coords')
        ds = ds.chunk(chunks={'time':100,'lat':681,'lon':841})
        for var in ds.data_vars.values():        
                comp = dict(zlib=True, complevel=9)
                encoding = {var.name: comp}
                var.to_netcdf(f'/scratch/er4/jr6311/final_mrnbc/awap/outputs/{var.name}_mrnbc_ACCESS1-0_rcp85.nc4', mode='w', engine='netcdf4',  encoding=encoding, unlimited_dims=['time'])
                # if not os.path.exists(f'{cfg["working_dir"]}/awap/outputs/{args.gcm}'):
                #         os.makedirs(f'{cfg["working_dir"]}/awap/outputs/{args.gcm}')
                # var.to_netcdf(f'{cfg["working_dir"]}/awap/outputs/{args.gcm}/{var.name}_{args.gcm}_{args.rcp}_mrnbc.nc4', mode='w', engine='netcdf4',  encoding=encoding, unlimited_dims=['time'])
                print(f'{var.name} has finished processing')

