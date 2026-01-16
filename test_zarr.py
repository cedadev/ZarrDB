import xarray as xr
ds = xr.open_dataset('https://gws-access.jasmin.ac.uk/public/eds_ai/era5_repack/zarr/ecmwf-era5_enda_an_sfc_19790101.mem0.10v.repack.zarr',engine='zarr')
print(ds.attrs)