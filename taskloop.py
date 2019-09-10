#!/usr/bin/env python
import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from os.path import exists

from aiojobs import create_scheduler
from app_state import state
from asyncio import gather, create_task, get_running_loop, as_completed
from click import Context, confirm, command, option, group, argument, progressbar
from loguru import logger

import environ
import click

Context.get_usage = Context.get_help  # show full help on error

import aiopool
import utils
import tasks.download
import tasks.merge
import tasks.tools

        
async def runtask(id, taskinfo, routine):
    """ Запустить таск, записать статус running/finished/failed в json. """
    file = Path(state._config.tasks_dir) / f'{id}.json'
    json.dump(dict(taskinfo, status='running'), open(file, 'w'), indent=2)
    
    tasks.tools.logger.bind(taskid=id)
    
    try:
        result = await routine
    except:
        taskinfo['status'] = 'failed'
        raise
    else:
        taskinfo['status'] = 'failed' if result is False else 'finished'
    finally:
        if taskinfo["status"] == 'failed':
            logger.info(f'Task {id} entered status "failed".')
        else:
            logger.debug(f'Task {id} entered status "finished".')
        json.dump(taskinfo, open(file, 'w'), indent=2)


async def spawn_export(pool, taskinfo, id):
    logger.debug(f'New export task "{taskinfo["cmd"]}"')
    await pool.spawn(runtask(id, taskinfo, tasks.tools.sh(
        taskinfo['cmd'], log_stdout=True, raise_error=False
    )))
        
        
async def spawn_merge(pool, taskinfo, id):
    camid = taskinfo['args'].get('camid')
    
    tmpdir = Path(state._config.tmp_merge_dir) / camid
    dstdir = Path(state._config.merged_dir) / camid
    if not exists(tmpdir): os.makedirs(tmpdir)
    if not exists(dstdir): os.makedirs(dstdir)
    
    tasks.tools.logger.bind(camid=camid)
    
    logger.debug(f'New merge task {camid}')
    await pool.spawn(runtask(id, taskinfo, tasks.merge.merge_camdir(
        srcdir = Path(state._config.downloaded_dir) / camid,
        tmp = tmpdir / f'{camid}.mp4',
        dst = dstdir / f'{camid}.mp4',
        force = state._config.force,
    )))
    
    
async def spawn_download(pool, taskinfo, id):
    camid = taskinfo['args'].get('camid')
    
    tmpdir = Path(state._config.tmp_download_dir) / camid
    dstdir = Path(state._config.downloaded_dir) / camid
    if not exists(tmpdir): os.makedirs(tmpdir)
    if not exists(dstdir): os.makedirs(dstdir)
    
    timestart = datetime.fromisoformat(taskinfo['args'].get('timestart'))
    
    logger.debug(f'New download task {camid} {timestart}')
    await pool.spawn(runtask(id, taskinfo, tasks.download.process_segment(
        camid,
        timestart,
        tmp = tmpdir / f'{camid}-{int(timestart.timestamp())}.flv',
        dst = dstdir / f'{camid}-{int(timestart.timestamp())}.flv',
        max_retries = state._config.max_download_retries,
        force = state._config.force
    )))
    #await pool.spawn(runtask(id, taskinfo, tasks.tools.sh(
        #f'./tasks/download.py --logfile="{state._config.tasks_dir}/{id}.log"'
        #f' --camid {camid}'
        #f' --timestart {timestart.isoformat()}'
        #f' --max-download-retries={state._config.max_download_retries}'
        #f' --tmp-download-dir={tmpdir}'
        #f' --dst-download-dir={dstdir}'
        #f' --loglevel={state._config.loglevel}' +
        #(f' --force' if state._config.force else ''),
        #raise_error=False,
    #)))
    
    
async def process_tasks(type, spawn_task, numworkers, **kw):
    """ Запустить все незаконченые таски заданного типа. """
    pool = aiopool.Pool(numworkers)
    
    tasks_dir = state._config.tasks_dir
    
    if not kw['restart_finished']:
        logger.info(f'{type}: Ignoring finished tasks. (use --restart-finished to override)')
    if not kw['restart_failed']:
        logger.info(f'{type}: Ignoring failed tasks. (use --restart-failed to override)')
    logger.debug(f'{type}: Scanning {tasks_dir} ...')
    
    try:
        for file in Path(tasks_dir).glob('*.json'):
            taskinfo = json.load(open(file))
            if not taskinfo['type'] == type:
                continue
            if taskinfo.get('status') == 'running':
                raise Exception(f'Task {file.stem} is running. Run `taskloop fail-running` first')
            if taskinfo.get('status') == 'finished' and not kw['restart_finished']:
                logger.debug(f'Skip finished task {file.stem}')
                continue
            if taskinfo.get('status') == 'failed' and not kw['restart_failed']:
                logger.debug(f'Skip failed task {file.stem}')
                continue
            await spawn_task(pool, taskinfo, id=file.stem)

        logger.info(f'Waiting for {len(pool.tasks)} {type} tasks to complete.')
        for n, task in enumerate(as_completed(pool.tasks)):
            result = await task
            logger.info(f'{n+1}/{len(pool.tasks)} tasks completed.')
    except:
        await pool.close()  # Отменить все таски.
        raise

    
@group('tasks')
@option('--loglevel', '-l', default='INFO',
            type=click.Choice(list(logging._nameToLevel), case_sensitive=False))
@option('--tasks-dir', type=click.Path(exists=True), required=True,
        help='Директория в которой нахдятся json-файлы описывающие таски.')
@option('--max-download-retries', type=int, default=2)
@option('--downloaded-dir', type=click.Path(), required=True,
        help='Директория куда помещаются все скачанные сегменты.')
@option('--merged-dir', type=click.Path(), required=True,
        help='Директория куда помещаются все склееные видео.')
def cli(**kw):
    """ Manage tasks. """
    state._config = kw
    tasks.tools.setup_logging(kw['loglevel'])
    
    
@cli.command('download', context_settings={'auto_envvar_prefix': 'CHURO'})
@option('--num-download-workers', type=int, default=1000)
@option('--tmp-download-dir', type=click.Path(), required=True)
@option('--force/--no-force', '-f', default=False, help='Обнулить счетчик попыток и качать заново.')
@option('--restart-finished', is_flag=True, default=False, 
            help='Перезапускать успешно завершенные ранее таски.')
@option('--restart-failed', is_flag=True, default=False, 
            help='Перезапускать неуспешно завершенные ранее таски.')
def tasks_download(**kw):
    """ Process download tasks from tasks dir. """
    state._config.update(kw)
    asyncio.run(process_tasks(
        'download', spawn_download, numworkers=kw['num_download_workers'], **kw
    ))
    

@cli.command('merge', context_settings={'auto_envvar_prefix': 'CHURO'})
@option('--num-merge-workers', type=int, default=2)
@option('--tmp-merge-dir', type=click.Path(), required=True)
#@option('--merge-split-on-gaps', is_flag=True, default=False,
            #help='Генерировать раздельные склееные файлы по местам разрывов.')
@option('--merge-08-20-tolerate-gaps-duration', type=int, default=120,
            help='Суммарное кол-во отсутствующих секунд с 8ч до 20ч допустимое для склеивания.')
@option('--merge-tolerate-incomplete-downloads', is_flag=True, default=False,
            help='Допускать склеивание недокачанных сегментов с разрывами.')
@option('--force/--no-force', '-f', default=False, help='Перезаписать файл если существует.')
@option('--restart-finished', is_flag=True, default=False, 
            help='Перезапускать успешно завершенные ранее таски.')
@option('--restart-failed', is_flag=True, default=False, 
            help='Перезапускать неуспешно завершенные ранее таски.')
def tasks_merge(**kw):
    """ Process merge tasks from tasks dir. """
    state._config.update(kw)
    asyncio.run(process_tasks(
        'merge', spawn_merge, numworkers=kw['num_merge_workers'], **kw
    ))
    
    
@cli.command('export', context_settings={'auto_envvar_prefix': 'CHURO'})
@option('--num-export-workers', type=int, default=1)
@option('--restart-finished', is_flag=True, default=False, 
            help='Перезапускать успешно завершенные ранее таски.')
@option('--restart-failed', is_flag=True, default=False, 
            help='Перезапускать неуспешно завершенные ранее таски.')
def tasks_export(**kw):
    """ Process export tasks from tasks dir. """
    state._config.update(kw)
    asyncio.run(process_tasks(
        'export', spawn_export, numworkers=kw['num_export_workers'], **kw
    ))
    
    
@cli.command('fail-running')
def tasks_failrunning(**kw):
    """ Mark all "running" tasks as "failed". """
    tasks_dir = state._config.tasks_dir
    
    logger.debug(f'Scanning {tasks_dir} ...')
    
    for file in Path(tasks_dir).glob('*.json'):
        task = json.load(open(file))
        if task.get('status') == 'running':
            logger.debug(f'Task {file.stem} invalidated')
            json.dump(dict(task, status='failed'), open(file, 'w'), indent=2)
        
        
if __name__ == '__main__':
    env = environ.Env()
    env.read_env('env-local')
    
    import uvloop
    uvloop.install()
    
    from signal import SIGINT, signal
    signal(SIGINT, utils.sigint_handler)

    cli(auto_envvar_prefix='CHURO')
    
    
