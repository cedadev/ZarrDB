# ZarrDB
Repository for scripts and configuration for the ZarrDB technology

# Startup

Activate MongoDB local database instance
```
docker run -d -p 27017:27017 -e MONGO_INITDB_ROOT_USERNAME=nraboy -e MONGO_INITDB_ROOT_PASSWORD=password1234 --name mongodb mongodb/mongodb-community-server:latest
```

Import kerchunk files via constructor

```
python constructor.py {filename}
```

Activate fastapi server
```
fastapi dev api.py
```

Open ZarrDB representation in Xarray using `engine=zarr`
```
import xarray as xr
ds = xr.open_dataset('http://localhost:8000/ds.zarr',engine='zarr')