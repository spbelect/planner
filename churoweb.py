
import asyncio
import json
from enum import Enum
from itertools import chain

from app_state import state
from fastapi import FastAPI, HTTPException
from loguru import logger
#from pydantic import BaseModel
from uvicorn import Server, Config

queue = asyncio.Queue()

app = FastAPI()

import tasks.download
import aiopool
import utils


async def process_queue(download=True, merge=True, export=True):
    from utils import stations
    from planner import plan_cams, spawn_download
    while True:
        plan = await queue.get()
        
        
        if plan.routine == 'download':
            if download:
                state._downloadpool = aiopool.Pool(state._config.num_download_workers)
                await plan_cams(plan, spawn_download)
                
                logger.info(f'Wating for {len(state._downloadpool.tasks)} tasks to complete.')
                succeeded, failed = 0, 0
                async for n, task in utils.as_completed(state._downloadpool.tasks):
                    result = await task
                    if result is False:
                        failed += 1
                    else:
                        succeeded += 1
                    if n % 1000 == 0:
                        logger.info(f'{n} of {len(state._downloadpool.tasks)} completed.'
                                    f' ({succeeded} ok, {failed} failed.)')
            else:
                print('ignoring download plan: ', plan)
                

class Routine(str, Enum):
    download = 'download'
    merge = 'merge'
    export = 'export'


@app.get("/plans/")
async def read_plans():
    """ Получить список планов. """
    return [x.as_dict(full=True) for x in state.plans.values()]


@app.delete("/plans/{id}")
async def delete_plan(id: int):
    """ Удалить план. """
    if id in state.plans:
        del state.plans[id]
    else:
        raise HTTPException(status_code=404, detail='no such plan')


@app.post("/plans/{id}/restart")
async def restart_plan(id: int):
    """ Перезапустить план. """
    plan = state.plans.get(id)
    if plan:
        plan.finished = False
        plan._force_restart = True
        queue.put_nowait(plan)
        return plan.as_dict()
    else:
        raise HTTPException(status_code=404, detail='no such plan')


@app.put("/plans/")
async def add_plan(
        routine: Routine, 
        region: int, 
        first_uik: int, 
        last_uik: int, 
        hour_start: int = 1, 
        hour_end: int = 2
    ):
    """ 
    Добавить план. 
    """
    id = max(state.plans or [0]) + 1
    state.plans[id] = dict(
        id = id,
        routine = str(routine),
        finished = False,
        region = region, 
        first_uik = first_uik, 
        last_uik = last_uik, 
        hour_start = hour_start, 
        hour_end = hour_end
    )
    queue.put_nowait(state.plans[id])
    return state.plans[id].as_dict()


server = Server(Config(app))
server.install_signal_handlers = lambda *a: None  # Do not catch signals
