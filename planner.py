#!/usr/bin/env python
import asyncio
import json
import logging
import os
import sys
from asyncio import gather, create_task, get_running_loop, as_completed
from datetime import timedelta, timezone
from os.path import exists
from pathlib import Path

import environ
import click
from app_state import state
from loguru import logger
from click import Context, confirm, command, option, group, argument
from psutil import Process

Context.get_usage = Context.get_help  # show full help on error

#from threadpool import Pool
import aiopool
import utils
import tasks.download
import tasks.merge
import tasks.tools
        
        
async def spawn_download(uik, camnum, camid, plan, tz):
    tmpdir = Path(state._config.tmp_download_dir) / camid
    dstdir = Path(state._config.downloaded_dir) / f'{plan["region"]}/{uik}-c{camnum}-{camid}'
    
    if not exists(tmpdir): os.makedirs(tmpdir)
    if not exists(dstdir): os.makedirs(dstdir)
    
    date = state._config.elect_date.replace(tzinfo=timezone(timedelta(hours=tz))) 
    
    log = tasks.tools.logger.bind(region=plan['region'], uik=uik, camnum=camnum)
    segments = []
    
    for hour in range(plan['hour_start'], plan['hour_end']):
        for minute in (0, 15, 30, 45):
            segments.append(await state._downloadpool.spawn(tasks.download.process_segment(
                camid, 
                dst = dstdir / f'{camid}-{hour}-{minute}-{plan["id"]}.flv',
                tmp = tmpdir / f'{camid}-{hour}-{minute}-{plan["id"]}.flv',
                timestart = date.replace(hour=hour, minute=minute),
                max_retries = state._config.max_download_retries,
                #force = plan, '_force_restart', state._config.force_download)
                force = state._config.force_download
            )))
            
    return segments

async def spawn_merge(uik, camnum, camid, plan, tz):
    log = tasks.tools.logger.get()
    srcdir = Path(state._config.downloaded_dir) / f'{plan.region}/{uik}-c{camnum}-{camid}'
    
    tmpdir = Path(state._config.tmp_merge_dir) / f'{plan.region}'
    dstdir = Path(state._config.merged_dir) / f'{plan.region}'
    if not exists(tmpdir): os.makedirs(tmpdir)
    if not exists(dstdir): os.makedirs(dstdir)
    
    job = await state._mergepool.spawn(tasks.merge.merge_camdir(
        srcdir,
        tmp = tmpdir / f'{uik}-c{camnum}-{camid}.mp4',
        dst = dstdir / f'{uik}-c{camnum}-{camid}.mp4',
        force = getattr(plan, '_force_restart', False)
    ))
    return [job]



    
async def plan_cams(plan, camroutine):
    plan._active = True
    logger.info(f'Processing new {plan.routine} plan: {plan}')
    
    if plan.region not in utils.stations():
        logger.warning(f'Plan#{plan.id} has unknown region {plan.region}.')
        return
    uiks = utils.stations()[plan.region]
    num_cams = 0
    jobs = []
    
    if not set(range(plan.first_uik, plan.last_uik + 1)) & set(uiks):
        logger.warning(f'Plan {plan.id}: No such uiks {plan.first_uik}-{plan.last_uik} in region {plan.region}.')
        
    for uik in sorted(uiks):
        if not (plan.first_uik <= uik <= plan.last_uik):
            continue
        tz = int(uiks[uik]['timezone_offset_minutes']) / 60
        for camnum, camid in enumerate(sorted(uiks[uik]['camera_id']), 1):
            num_cams += 1
            jobs += (await camroutine(uik, camnum, camid, plan, tz))
        
    logger.info(f'Plan {plan.id}: {num_cams} cameras to process. ({len(jobs)} jobs)')
    #asyncio.gather(*[x.wait() for x in jobs])
    create_task(planwatch(plan, jobs))
    

async def planwatch(plan, jobs):
    try:
        await asyncio.gather(*[x._do_wait(timeout=None) for x in jobs])
    except Exception as e:
        plan._active = False
        logger.error(f'Plan {plan.id} failed: child raised {e!r}')
        return
    
    plan._active = False
    plan.finished = True
    logger.info(f'Plan finished {plan}')
    

    
async def plans_run(download, merge, export):
    import churoweb
    #asyncio.set_child_watcher(asyncio.FastChildWatcher())
    
    state._num_terminated = 0
    
    churoweb.queue = asyncio.Queue()
    server = create_task((churoweb.server.serve()))
    
    finished = [x for x in state.plans.values() if x.finished]
    print(f'Ignoring {len(finished)} finished plans.')
    
    unfinished = [x for x in state.plans.values() if not x.finished]
    print(f'{len(unfinished)} plans to process.')
    
    try:
        if download:
            state._downloadpool = aiopool.Pool(state._config.num_download_workers)
            #state._downloadpool = aiopool.Pool(1000)
            plans = [x for x in unfinished if x.routine == 'download']
            await gather(*[
                create_task(plan_cams(plan, spawn_download)) for plan in plans
            ])
            succeeded, failed = 0, 0
            logger.info(f'Wating for {len(state._downloadpool.tasks)} tasks to complete.')
            async for n, task in utils.as_completed(state._downloadpool.tasks):
                result = await task
                if result is False:
                    failed += 1
                else:
                    succeeded += 1
                if n % 1000 == 0:
                    logger.info(f'{n} of {len(state._downloadpool.tasks)} completed.'
                                f' ({succeeded} ok, {failed} failed.)')
            logger.info('All download plans finished.')
            
        if merge:
            state._mergepool = aiopool.Pool(state._config.num_merge_workers)
            plans = [x for x in unfinished if x.routine == 'merge']
            await gather(*[
                create_task(plan_cams(plan, spawn_merge)) for plan in plans
            ])
            await gather(*state._mergepool.tasks)
            
        #if export:
            #plans = [x for x in unfinished if x.routine == 'export']
            #await gather(*[
                #create_task(plan_cams(plan, tasks.export.export)) for plan in plans
            #])
    except:
        logger.warning('Error was raised by child. Cancelling all tasks.')
        await state._downloadpool.close()  # Отменить все таски.
        logger.warning(f'{state._num_terminated} subprocesses terminated.')
        logger.warning(f'{len(Process().children(recursive=True))} child processes still running.')
        logger.warning('Stopping web server.')
        churoweb.server.should_exit = True
        await server
        logger.warning('Exit.')
        raise
    
    logger.info('All plans finished. Press ctrl-c to quit webserver.')
    create_task(churoweb.process_queue(download, merge, export))
    await server

 
@group('planner')
@option('--elect-date', required=True, type=click.DateTime(['%Y-%m-%d']),
        help='Дата проведения выборов формата YYYY-MM-DD')
@option('--loglevel', '-l', default='INFO',
        type=click.Choice(list(logging._nameToLevel), case_sensitive=False))
def cli(loglevel, **kwargs):
    """ Manage plans. """
    tasks.tools.setup_logging(loglevel)
    state._config = kwargs
    state.autopersist('state.shelve', timeout=1)
    state.setdefault('plans', {})
    

@cli.command('run', context_settings={'auto_envvar_prefix': 'CHURO'})
@option('--download', is_flag=True, default=True)
@option('--merge', is_flag=True, default=False)
@option('--export', is_flag=True, default=False)
# Download options
@option('--tmp-download-dir', type=click.Path(), required=True)
@option('--downloaded-dir', type=click.Path(), required=True)
@option('--num-download-workers', type=int, default=1000)
@option('--max-download-retries', type=int, default=2)
@option('--force-download/--no-force-download', '-fd', default=False, help='Обнулить счетчик попыток')
# Merge options
@option('--tmp-merge-dir', type=click.Path(), required=True)
@option('--merged-dir', type=click.Path(), required=True)
@option('--num-merge-workers', type=int, default=2)
@option('--merge-split-on-gaps', is_flag=True, default=False,
        help='Генерировать раздельные склееные файлы по местам разрывов.')
@option('--merge-08-20-tolerate-gaps-duration', type=int, default=120,
        help='Суммарное кол-во отсутствующих секунд допустимое для склеивания.')
@option('--merge-tolerate-incomplete-downloads', is_flag=True, default=False,
        help='Допускать склеивание недокачанных сегментов с разрывами.')
def cli_plans_run(download, merge, export, **kw):
    """ Process all unfinished plans. """
    state._config.update(kw)
    asyncio.run(plans_run(download, merge, export))
    
    
@cli.command('rm')
@argument('id', type=int)
def plans_rm(id):
    """ Delete plan. """
    del state.plans[id]
    
    
@cli.command('restart')
@argument('id', type=int)
def plans_restart(id):
    """ Set plan "finished" = false. """
    state.plans[id].finished = False
    print(f'Plan {id} successfully changed "finished" to "false"')
    #print(json.dumps(state.plans[id], indent=2))
    
    
@cli.command('show')
def plans_show(**kw):
    """ Print current plans. """
    print(json.dumps([x.as_dict(full=True) for x in state.plans.values()], indent=2))
    
    
@cli.command('add', context_settings={'auto_envvar_prefix': 'CHURO_PLANS_ADD'})
@option('--routine', default='download', type=click.Choice(['download','merge','export']), prompt=True)
@option('--region', type=int, default=1, prompt=True)
@option('--first-uik', type=int, default=1, prompt=True)
@option('--last-uik', type=int, default=2, prompt=True)
@option('--hour-start', type=int, default=7, prompt=True)
@option('--hour-end', type=int, default=8, prompt=True)
def plans_add(**kw):
    """ Add plan. """
    id = max(state.plans or [0]) + 1
    state.plans[id] = dict(kw, id=id, finished=False)
    print(json.dumps(state.plans[id], indent=2))
    
    
@cli.group('uiks')
@argument('uiks', nargs=-1)
def cli_uiks(**kw):
    """
    Download or merge uiks.
    """
    
    
@cli_uiks.command('download')
@option('--region', '-rn', prompt=True, help='Region number')
@option('--turnout_min', '-tumin', default=0)
@option('--turnout_max', '-tumax', default=100)
#@option('--timestart', '-ts', default='07-45')
#@option('--timeend', '-te', default='20-00')
@option('--force', '-f', is_flag=True, default=False)
#@argument('uiks', nargs=-1, required=True)
def uiks_download(*args):
    """
    For each uik/camera, download video.
    TODO
    """
    #asyncio.run(download(*args))
    
    
@cli_uiks.command('merge')
@option('--timestart', '-ts', default='07-45')
@option('--timeend', '-te', default='20-00')
def uiks_merge(*args):
    """
    For each uik/camera, merge video.
    TODO
    """
    #asyncio.run(merge.main(*args))
    
    
if __name__ == '__main__':
    env = environ.Env()
    env.read_env('env-local')
    
    import uvloop
    uvloop.install()

    from signal import SIGINT, signal
    signal(SIGINT, utils.sigint_handler)    
    
    cli(auto_envvar_prefix='CHURO')
    
    
