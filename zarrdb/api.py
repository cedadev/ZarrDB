from fastapi import FastAPI, HTTPException, Response
import pymongo
import json
import os

import yarl
import aiohttp
import base64
import logging

from zarrdb.utils import zarrdata, app, logstream, nfiles

logger = logging.getLogger('zarrdb.' + __name__)
logger.addHandler(logstream)
logger.propagate = False

session = None

revision = '1.1'

def check_exists(zarr_ds) -> None:
    zarr_datasets = zarrdata.list_collection_names()

    zdb = zarr_ds.replace(f'z{revision}.zarr',f'zdb{revision}.json')

    if zarr_ds not in zarr_datasets or not os.path.isfile(f'configs/zdb/{zdb}'):
        raise HTTPException(status_code=404, detail='Zarr DS not found')

@app.on_event('startup')
async def startup_event():
    global session
    session = aiohttp.ClientSession()

@app.on_event('shutdown')
async def shutdown_event():
    await session.close()

@app.get('/')
def read_root():
    return zarrdata.list_collection_names()

@app.get('/{zarr_ds}/.zgroup')
def read_zarr_group(zarr_ds: str):
    check_exists(zarr_ds)

    zdb = zarr_ds.replace(f'z{revision}.zarr',f'zdb{revision}.json')
    with open(f'configs/zdb/{zdb}') as f:
        return json.load(f)['refs']['.zgroup']

@app.get('/{zarr_ds}/.zattrs')
def read_zarr_attrs(zarr_ds: str):
    check_exists(zarr_ds)

    zdb = zarr_ds.replace(f'z{revision}.zarr',f'zdb{revision}.json')
    with open(f'configs/zdb/{zdb}') as f:
        return json.load(f)['refs']['.zattrs']

@app.get('/{zarr_ds}/.zmetadata')
def read_zarr_meta(zarr_ds: str):
    check_exists(zarr_ds)

    zdb = zarr_ds.replace(f'z{revision}.zarr',f'zdb{revision}.json')
    with open(f'configs/zdb/{zdb}') as f:
        refs = json.load(f)['refs']
        return {'metadata':refs}

async def read_kerchunk_ref(url, kw):
    async with session.get(url, **kw) as r:
        out = await r.read()
    return out

@app.get('/{zarr_ds}/{var}/{chunk_id}')
async def read_zarr_data(zarr_ds: str, var: str, chunk_id: str):
    check_exists(zarr_ds)
    
    try:
        # Key-value mapping
        chunk_refs = zarrdata[zarr_ds].find({"_id": f"{var}/{chunk_id}"})[0]
    except IndexError:
        raise HTTPException(status_code=404, detail=f'Chunk {var}/{chunk_id} unavailable')
    
    if chunk_refs.get('d'):
        data = base64.b64decode(chunk_refs['d'][7:])
        logger.info(var, chunk_id, 'b64')
    else:

        # Default no headers
        kw = {}
        if chunk_refs.get('o'):

            lim0 = int(chunk_refs['o'])
            lim1 = int(chunk_refs['o']) + int(chunk_refs['s'])

            url = yarl.URL(
                nfiles[
                    zarr_ds.replace('.zarr','.nfs')].find(
                        {'_id':chunk_refs['h']}
                    )
                [0]['h'])

            kw   = {'headers': {'Range': f'bytes={lim0}-{lim1-1}'}}

        # Able to request whole objects if needed
        data = await read_kerchunk_ref(url, kw)

        logger.info(var, chunk_id, kw)

    # Data response - mimics Object Store requests
    return Response(content=data, media_type='application/octet-stream')
