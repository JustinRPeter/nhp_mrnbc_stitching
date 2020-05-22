from xclim import subset
import xarray as xr
import numpy as np
from multiprocessing import Pool

import glob
import os

def subdivide_cdf(ds, lat_bnds, lon_bnds, path):
    new_ds = subset.subset_bbox(ds,lat_bnds=lat_bnds,lon_bnds=lon_bnds)
    new_ds.to_netcdf(f'{path}.nc4')

def iterate_cdfs(cdf):
    gcmlist = ['CNRM-CM5', 'MIROC5', 'ACCESS1-0', 'GFDL-ESM2M']
#    gcmlist = ['MIROC5']
    ds = xr.open_dataset(f'{cdf}')
    pathinfo = os.path.basename(os.path.splitext(cdf)[0]).split('_')
    if pathinfo[0] in gcmlist:
        return True
    if not os.path.exists(f'/scratch/er4/jr6311/mrnbc/final_outputs/sda_prep_new/{pathinfo[1]}/{pathinfo[0]}_{pathinfo[2]}/'):
        os.makedirs(f'/scratch/er4/jr6311/mrnbc/final_outputs/sda_prep_new/{pathinfo[1]}/{pathinfo[0]}_{pathinfo[2]}/')

    for lats in reversed(range(1, ds.dims['lat']-1)):
        for lons in range(1, ds.dims['lon'] - 1):
            lonvals = [float(ds['lon'][lons -1]), float(ds['lon'][lons +1])]
            latvals = [float(ds['lat'][lats -1]), float(ds['lat'][lats +1])]
            subdivide_cdf(ds, latvals, lonvals, f'/scratch/er4/jr6311/mrnbc/final_outputs/sda_prep_new/{pathinfo[1]}/{pathinfo[0]}_{pathinfo[2]}/{lons}_{(ds.dims["lat"] -1) - lats}')
#             subdivide_cdf(ds, latvals, lonvals, f'/g/data/er4/jr6311/unsw-bias-correction/final_outputs/sda_prep_new/{pathinfo[1]}/{pathinfo[0]}_{pathinfo[2]}/{lons}_{lats}')


if __name__ == '__main__':
    cdflist = glob.glob('/scratch/er4/jr6311/mrnbc/final_outputs/bc_stitched/*/*')

    # iterate_cdfs('/g/data/er4/jr6311/unsw-bias-correction/new_outputs/bc_stitched/GFDL-ESM2M/tasmin_GFDL-ESM2M_unsw-bc-cur_stitched.nc4')
#    for i in cdflist:
#        iterate_cdfs(i)
    with Pool() as p:
        p.map(iterate_cdfs, cdflist)
