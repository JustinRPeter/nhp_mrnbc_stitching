import xarray as xr
import dask as da
import glob

from dask.distributed import Client


if __name__ == "__main__":
        da.config.config
        da.config.set({'temporary-directory': '/scratch/er4/jr6311/dask_temp'})
        da.config.set({'dataframe':{'shuffle-compression': 'Zlib'}})
        client = Client(n_workers=20)

        print(client)
        
        flist = glob.glob(f'/g/data/er4/jr6311/mrnbc/stitch_ACCESS1-0_rcp85/*')
        ds = xr.open_mfdataset(flist, chunks={'lat':10, 'lon':10}, combine='by_coords')
        ds = ds.chunk(chunks={'time':100,'lat':681,'lon':841})
        for var in ds.data_vars.values():        
                comp = dict(zlib=True, complevel=9)
                encoding = {var.name: comp}
                var.to_netcdf(f'/scratch/er4/jr6311/final_mrnbc/awap/outputs/{var.name}_mrnbc_ACCESS1-0_rcp85.nc4', mode='w', engine='netcdf4',  encoding=encoding, unlimited_dims=['time'])
                print(f'{var.name} has finished processing')
