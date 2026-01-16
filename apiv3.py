from fastapi import FastAPI, HTTPException, Response
import pymongo
from bson.objectid import ObjectId
import requests
import yarl
import aiohttp

EXAMPLE_ZARR = {
  "attributes": {
    "Conventions": "CF-1.6",
    "history": "See individual files for more details",
    "aggregation": "padocc"
  },
  "zarr_format": 3,
  "consolidated_metadata": {
    "kind": "inline",
    "must_understand": False,
    "metadata": {
      "longitude": {
        "shape": [
          1440
        ],
        "data_type": "float32",
        "chunk_grid": {
          "name": "regular",
          "configuration": {
            "chunk_shape": [
              1440
            ]
          }
        },
        "chunk_key_encoding": {
          "name": "default",
          "configuration": {
            "separator": "/"
          }
        },
        "fill_value": 0.0,
        "codecs": [
          {
            "name": "bytes",
            "configuration": {
              "endian": "little"
            }
          }
        ],
        "attributes": {
          "units": "degrees_east",
          "long_name": "longitude",
          "_FillValue": "AAAAAAAA+H8="
        },
        "dimension_names": [
          "longitude"
        ],
        "zarr_format": 3,
        "node_type": "array",
        "storage_transformers": []
      },
      "v10": {
        "shape": [
          8,
          721,
          1440
        ],
        "data_type": "float64",
        "chunk_grid": {
          "name": "regular",
          "configuration": {
            "chunk_shape": [
              1,
              721,
              1440
            ]
          }
        },
        "chunk_key_encoding": {
          "name": "default",
          "configuration": {
            "separator": "/"
          }
        },
        "fill_value": 0.0,
        "codecs": [
          {
            "name": "bytes",
            "configuration": {
              "endian": "little"
            }
          }
        ],
        "attributes": {
          "units": "m s**-1",
          "long_name": "10 metre V wind component",
          "_FillValue": "AAAAAAAA+H8="
        },
        "dimension_names": [
          "time",
          "latitude",
          "longitude"
        ],
        "zarr_format": 3,
        "node_type": "array",
        "storage_transformers": []
      },
      "latitude": {
        "shape": [
          721
        ],
        "data_type": "float32",
        "chunk_grid": {
          "name": "regular",
          "configuration": {
            "chunk_shape": [
              721
            ]
          }
        },
        "chunk_key_encoding": {
          "name": "default",
          "configuration": {
            "separator": "/"
          }
        },
        "fill_value": 0.0,
        "codecs": [
          {
            "name": "bytes",
            "configuration": {
              "endian": "little"
            }
          }
        ],
        "attributes": {
          "units": "degrees_north",
          "long_name": "latitude",
          "_FillValue": "AAAAAAAA+H8="
        },
        "dimension_names": [
          "latitude"
        ],
        "zarr_format": 3,
        "node_type": "array",
        "storage_transformers": []
      },
      "time": {
        "shape": [
          8
        ],
        "data_type": "int32",
        "chunk_grid": {
          "name": "regular",
          "configuration": {
            "chunk_shape": [
              1
            ]
          }
        },
        "chunk_key_encoding": {
          "name": "default",
          "configuration": {
            "separator": "/"
          }
        },
        "fill_value": 0,
        "codecs": [
          {
            "name": "bytes",
            "configuration": {
              "endian": "little"
            }
          }
        ],
        "attributes": {
          "long_name": "time",
          "units": "hours since 1900-01-01",
          "calendar": "gregorian"
        },
        "dimension_names": [
          "time"
        ],
        "zarr_format": 3,
        "node_type": "array",
        "storage_transformers": []
      }
    }
  },
  "node_type": "group"
}

app = FastAPI()
username = 'nraboy'
password = 'password1234'

dbclient = pymongo.MongoClient('mongodb://%s:%s@127.0.0.1' % (username, password))
zarrdb = dbclient['zarr']

session = None

@app.on_event('startup')
async def startup_event():
    global session
    session = aiohttp.ClientSession()

@app.on_event('shutdown')
async def shutdown_event():
    await session.close()

@app.get('/')
def read_root():
    return {"Hello":"World"}

@app.get('/{zarr_ds}/zarr.json')
def read_zarr_group(zarr_ds: str):
    zarr_datasets = zarrdb.list_collection_names()
    if zarr_ds not in zarr_datasets:
        raise HTTPException(status_code=404, detail='Zarr DS not found')
    
    return EXAMPLE_ZARR

async def read_kerchunk_ref(url, kw):
    async with session.get(url, **kw) as r:
        out = await r.read()
    return out

@app.get('/{zarr_ds}/{var}/c/{chunk_id}')
async def read_zarr_data(zarr_ds: str, var: str, chunk_id: str):
    zarr_datasets = zarrdb.list_collection_names()
    if zarr_ds not in zarr_datasets:
       raise HTTPException(status_code=404, detail='Zarr DS not found')
    
    chunk_refs = zarrdb[zarr_ds].find({"_id": f"{var}/{chunk_id}"})[0]

    lim0 = int(chunk_refs['offset'])
    lim1 = int(chunk_refs['offset']) + int(chunk_refs['size'])
    url  = yarl.URL(chunk_refs['href'])

    kw = {'headers': {'Range': f'bytes={lim0}-{lim1}'}}

    data = await read_kerchunk_ref(url, kw)

    data2 = requests.get('https://gws-access.jasmin.ac.uk/public/eds_ai/era5_repack/zarr/ecmwf-era5_enda_an_sfc_19790101.mem0.10v.repack.zarr/time/c/0').content

    for x in zarrdb[zarr_ds].find():
        print(x)

    print(type(data))
    print(type(data2))

    return Response(content=data, media_type='application/octet-stream')
