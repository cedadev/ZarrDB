import pymongo
import json
import sys

username = 'nraboy'
password = 'password1234'

dbclient = pymongo.MongoClient('mongodb://%s:%s@127.0.0.1' % (username, password))
zarrdb = dbclient['zarr']

kfile = sys.argv[-1]
proj_code = kfile.split('.kr')[0]
revision = kfile.split('.kr')[-1].replace('.json','')

example_zarr = zarrdb[proj_code + '.z' + revision + '.zarr']

reduced_meta = {'version':None, 'refs':{}}
with open(f'kerchunk/{kfile}') as f:
    refs = json.load(f)

example_refs = []
reduced_meta['version'] = refs['version']
for k, v in refs['refs'].items():
    if len(v) == 3 and isinstance(v,list):
        example_refs.append(
            {'_id':k, 'href': v[0], 'offset':v[1], 'size':v[2]}
        )
    elif 'base64' in v:
        example_refs.append(
            {'_id':k, 'data': v})
    else:
        if isinstance(v, str):
            v = json.loads(v)

        for k2,v2 in v.items():
            if isinstance(v2, str):
                if '\u003C' in v2:
                    v[k2] = v2.replace('\u003C','<')
            if str(v2) == 'nan':
                v[k2] = None
        reduced_meta['refs'][k] = v

x = example_zarr.insert_many(example_refs)

with open(f'zarrdb/{proj_code}.zdb{revision}.json','w') as f:
    f.write(json.dumps(reduced_meta))