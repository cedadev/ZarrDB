import xarray as xr
ds = xr.open_dataset('https://gws-access.jasmin.ac.uk/public/eds_ai/era5_repack/sample_aggregation/data/ecmwf-era5_enda_an_sfc_19790101.mem0.10v.repack.kr1.0.json',engine='kerchunk')
print(ds.attrs)