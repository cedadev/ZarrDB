from fastapi import FastAPI, HTTPException, Response
import pymongo
import json
import os

import yarl
import aiohttp
import base64

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
    return zarrdb.list_collection_names()

@app.get('/{zarr_ds}/.zgroup')
def read_zarr_group(zarr_ds: str):
    zarr_datasets = zarrdb.list_collection_names()

    zdb = zarr_ds.replace('z1.0.zarr','zdb1.0.json')

    if zarr_ds not in zarr_datasets or not os.path.isfile(f'zarrdb/{zdb}'):
        raise HTTPException(status_code=404, detail='Zarr DS not found')
    
    with open(f'zarrdb/{zdb}') as f:
        return json.load(f)['refs']['.zgroup']

@app.get('/{zarr_ds}/.zattrs')
def read_zarr_attrs(zarr_ds: str):
    zarr_datasets = zarrdb.list_collection_names()
    zdb = zarr_ds.replace('z1.0.zarr','zdb1.0.json')

    if zarr_ds not in zarr_datasets or not os.path.isfile(f'zarrdb/{zdb}'):
        raise HTTPException(status_code=404, detail='Zarr DS not found')
   
    with open(f'zarrdb/{zdb}') as f:
        return json.load(f)['refs']['.zattrs']

@app.get('/{zarr_ds}/.zmetadata')
def read_zarr_meta(zarr_ds: str):
    zarr_datasets = zarrdb.list_collection_names()
    zdb = zarr_ds.replace('z1.0.zarr','zdb1.0.json')

    if zarr_ds not in zarr_datasets or not os.path.isfile(f'zarrdb/{zdb}'):
        raise HTTPException(status_code=404, detail='Zarr DS not found')
   
    with open(f'zarrdb/{zdb}') as f:
        return {'metadata':json.load(f)['refs']}

async def read_kerchunk_ref(url, kw):
    async with session.get(url, **kw) as r:
        out = await r.read()
    return out

@app.get('/{zarr_ds}/{var}/{chunk_id}')
async def read_zarr_data(zarr_ds: str, var: str, chunk_id: str):
    zarr_datasets = zarrdb.list_collection_names()
    zdb = zarr_ds.replace('z1.0.zarr','zdb1.0.json')

    if zarr_ds not in zarr_datasets or not os.path.isfile(f'zarrdb/{zdb}'):
       raise HTTPException(status_code=404, detail='Zarr DS not found')
    
    try:
        chunk_refs = zarrdb[zarr_ds].find({"_id": f"{var}/{chunk_id}"})[0]
    except IndexError:
        raise HTTPException(status_code=404, detail=f'Chunk {var}/{chunk_id} unavailable')
    
    if chunk_refs.get('data'):
        data = base64.b64decode(chunk_refs['data'][7:])
        print(var, chunk_id, 'b64')
    else:

        lim0 = int(chunk_refs['offset'])
        lim1 = int(chunk_refs['offset']) + int(chunk_refs['size'])
        url  = yarl.URL(chunk_refs['href'])

        kw = {'headers': {'Range': f'bytes={lim0}-{lim1-1}'}}

        data = await read_kerchunk_ref(url, kw)

        # Range-get object -> ? -> zarr data file.
        print(var, chunk_id, kw)
        print(data)
    return Response(content=data, media_type='application/octet-stream')
