import asyncio
import json
import os

from collections import defaultdict, namedtuple
from os.path import exists, isdir, join, dirname
from pathlib import Path
from app_state import state
from loguru import logger


def makedirs(*parts):
    absdir = join(*parts)
    try:
        os.makedirs(absdir)
    except os.error:
        if not isdir(absdir):
            raise
    return absdir

 
_stations = defaultdict(dict) 

def stations():
    global _stations
    if not _stations:
        for x in json.load(open(Path(__file__).parent / 'stations.json')):
            _stations[x.get('region_number')][x.get('station_number')] = x
    return _stations


async def as_completed(tasks):
    n = 0
    pending = list(tasks)
    while pending:
        done, pending = await asyncio.wait(pending, timeout=10, return_when=asyncio.FIRST_COMPLETED)
        #logger.info(f'{len(done)} done, {len(pending)} pending')
        for x in done:
            x.exception()  # Consume exception to prevent loging flood
            
        for x in done:
            n += 1
            yield n, x


def sigint_handler(*a):
    logger.warning('Got SIGINT. Stopping event loop.')
    
    # This flag is checked in aiopool
    # See https://github.com/MagicStack/uvloop/issues/243
    state._stopping = True 
    
    asyncio.get_running_loop().stop()
    
