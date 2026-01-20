# Class to gather concurrent requests asynchronously and process in batches.

# Define the first request as 'master' process which waits for 0.5 seconds to allow other requests,
# then asyncio.gather on found processes.

# Subsequent requests (queued) submit their data to the gather class in some way and await a response
# How to make the 'await' work for a class method i.e await Gatherer.fetch(my_chunk) such that when the master
# process concludes, all fetch processes unlock?

import asyncio

async def call_url(session, url, headers):
    resp = await session.request(method='GET', url=url, headers=headers)
    return resp.content

class Gatherer:

    def __init__(self, session):
        self.event = asyncio.Event()
        self.master = None

        self.session = session
        
        self.chunk_register = {}
        self.chunk_data = {}

    async def register(self, chunk, url, headers):

        self.chunk_register[chunk] = (url, headers)

        if self.master is None:

            self.master = True
            await asyncio.sleep(0.5)
            await self.fetch_all()
            self.master = None

    async def fetch_all(self):
        chunk_data = await asyncio.gather(*[call_url(self.session, u, h) for u, h in self.chunk_register.values()])
        self.chunk_data = {k:c for k,c in zip(self.chunk_register.keys(),chunk_data)}
        self.event.set()

    async def fetch(self, chunk):

        if chunk in self.chunk_data:
            return self.chunk_data[chunk]
    
        while True: # For now
            await self.event.wait()
            if chunk in self.chunk_data:
                return self.chunk_data[chunk]
            self.event.clear()


        

    