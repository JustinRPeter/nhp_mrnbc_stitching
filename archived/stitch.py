import numpy as np
import pandas as pd
import glob
import os
import xarray as xr
import concurrent.futures


# parser = argparse.ArgumentParser() # Read the command line, setting reasonable default values.
# parser.add_argument("-m", "--model_id", choices="access1_0, cnrm_cm5, gfdl_esm2", help="Provide a model.")
# parser.add_argument("-t", "--t_period", choices="bc_cur, bc_fut", help="Provide a time period.")
# args = parser.parse_args()

model_id = "cnrm_cm5"
t_span = "bc_fut"
t_period = t_span

if model_id == "access1_0":
    model_dir = "ACCESS1-0"
elif model_id == "cnrm_cm5":
    model_dir = "CNRM-CM5"
elif model_id == "gfdl_esm2m":
    model_dir = "GFDL-ESM2M"
else:
    print ('Please define a model.')

# if t_span == "bc_cur":
#     t_period = "bc_cur"
# elif t_span == "bc_fut":
#     t_period = "bc_fut"
# else:
#     print ("Please define timespan.")

def discover_files(model_dir, t_period):
    bc_datasets = glob.glob(f'/g/data/er4/jp0715/HydroProj/data/mrnbc_output/{model_dir}/*/{t_period}*.dat')
    return bc_datasets

def open_dataframe(open_file):
    '''
    opens first open_file in list and loads it as a dataframe in pandas.
    loaded dataframe has its columns named based on pridictable input.
    new date column is created from year, month and day columns (which subsequently then deleted)
    '''
    # open_df = discover_files(model_dir, t_period).pop(0)
    load_df = pd.DataFrame(np.loadtxt(open_file))
    load_df.columns = ['year', 'month', 'day', 'pr', 'tasmax', 'tasmin', 'sfcWind', 'rsds']
    load_df['time'] = load_df.apply(lambda row: pd.Timestamp(year=int(row[0]), month=int(row[1]), day=int(row[2])), axis=1)
    ordered_df = load_df.drop(['year', 'month', 'day'], axis=1)
    return ordered_df

def find_lon_lat_step(open_df):
    # filename, ext = os.path.basename(open_df).split('.')
    # file_attributes = [x for x in filename.split("_")]
    # _, _, ilon, ilat = filename

    # if computing gcmc run this method:
    ilon, ilat = os.path.basename(os.path.split(open_df)[0]).split('_')

    ilon = float (ilon)
    ilat = float (ilat)
    return ilon, ilat

class Coord:

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
    initial = Coord(lon = 108.75, lat = -6.303452187571035)
    step = Coord(lon=1.40625, lat=-1.40076385695359)
    nstep = Coord(lon=ilon, lat=ilat)
    offset = initial.offset(step, nstep)
    # print(f"{offset.lon} {offset.lat}")
    return offset.lon, offset.lat

def fix_the_indicies(ordered_df, lon, lat):
    ordered_df['lon'] = lon
    ordered_df['lat'] = lat
    primed_df = ordered_df.set_index(['lat', 'lon', 'time'])
    return primed_df

def process_file(open_file):
    ordered_df = open_dataframe(open_file)
    ilon, ilat = find_lon_lat_step(open_file)
    lon, lat = coords_from_path(ilon, ilat)
    df2xr = fix_the_indicies(ordered_df, lon, lat)
    xr_dataset = df2xr.to_xarray()
    outpath = f"/g/data1a/er4/jr6311/unsw-bias-correction/data/{model_dir}/{t_period}"
    os.makedirs(outpath, exist_ok=True)
    xr_dataset.to_netcdf(path=f"{outpath}/{int(ilon)}_{int(ilat)}.nc", mode='w', engine='netcdf4')

if __name__ == "__main__":
    with concurrent.futures.ProcessPoolExecutor() as executor:
        dat_files = discover_files(model_dir, t_period)
        for open_file, _ in zip(dat_files, executor.map(process_file, dat_files)):
            continue

        print ("Complete.")
