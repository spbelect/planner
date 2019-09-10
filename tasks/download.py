#!/usr/bin/env python
import os
import sys

import asyncio
import json
import logging
import re
import shutil
from datetime import datetime, timezone, timedelta
from os.path import exists, isdir, join, dirname, abspath
from pathlib import Path

from asyncio import create_task, gather, get_event_loop

import aiojobs
import click
from app_state import state
from click import Context, confirm, command, option, group, argument
try:
    from tools import sh, logger, setup_logging, sum_gaplength
except ImportError:
    from .tools import sh, logger, setup_logging, sum_gaplength


Context.get_usage = Context.get_help  # show full help on error

    
async def download_segment(file, camid, timestart):
    """
    TODO
    Запустить внешний процесс скачивания сегмента.
    """
    utctime = timestart.astimezone(timezone.utc).isoformat()
    cmd = f'touch {file}; sleep 15;'  # TODO
    await sh(cmd)

    
async def timestamps(file):
    """ Iterate over Presentation Timestamps of packets. """
    cmd = 'ffprobe -loglevel error -hide_banner -of compact' \
          ' -select_streams v:0 -show_entries packet=pts_time ' + str(file)
      
    cmd = 'echo "packet|pts_time=123.456\npacket|pts_time=78.456"'  # TODO: stub
    
    stdout = await sh(cmd)
    for line in stdout.split():
        if line.strip():
            yield float(re.search(b'packet\|pts_time=(\d+.\d+)', line).group(1))
       
       
async def check_file(file, localtime, duration=900, maxdiff=2):
    """ Проверить файл и вернуть gapreport """
    #return {'localtime': localtime.isoformat()}
    ts = [x async for x in timestamps(file)]
    
    report = {'localtime': localtime.isoformat()}
    if not ts:
        report['invalid_file'] = True
        return report
    
    if ts[-1] < duration:
        report['duration_error'] = ts[-1]
        
    diffs = [y - x for x, y in zip(ts[:-1], ts[1:])]
    for diff in diffs:
        if diff > maxdiff:
            report.setdefault('maxdiff_errors', []).append({
                'start': int(ts[diffs.index(diff)]), 'len': diff
            })
    return report
    
    
#@log_exception
async def process_segment(camid, timestart, tmp, dst, max_retries, force=False):
    """
    Скачать 15-минутный сегмент камеры camid, начинающийся со времени timestart во 
    временную папку, проверить на разрывы. 
    Если разрывов меньше чем в существующем файле, перезаписать его.
    Если есть разрывы - повторять скачивание max-download-retries раз.
    Сохранить отчет о разрывах в {segmentvideofile}.flv.gapreport.json
    Если force == False то продолжаем считать кол-во попыток с прошлого запуска.
    Если force == True то кол-во попыток с прошлого запуска обнуляется.
    Возвращает True если сегмент скачан без разрывов.
    """
    #raise Exception('lol')
    log = logger.bind(segment_time=timestart, camid=camid)
    dst_gaplength = None
    prevattempts = 0
    gapfile = str(dst) + '.gapreport.json'  # отчет о разрывах
    if exists(gapfile):
        try:
            report = json.load(open(gapfile))
        except:
            log.error(f'Malformed gapreport file {gapfile}.')
        else:
            if force:
                report['attempts'] = 0
            else:
                prevattempts = report['attempts']
                
            dst_gaplength = sum_gaplength(report)
            if dst_gaplength == 0:
                log.debug('Сегмент был скачан ранее, и не содержит разрывов.')
                return True
            elif report['attempts'] >= max_retries:
                log.debug('Сегмент был скачан ранее c разрывами, осталось 0 попыток. (требуется --force)')
                return False
            log.debug(f'Сегмент был скачан ранее c разрывами, осталось {max_retries - prevattempts} попыток.')
    
    for attempt in range(prevattempts + 1, max_retries + 1):
        log.debug(f'Starting download, attempt {attempt}.')
        await download_segment(tmp, camid, timestart)
        report = await check_file(tmp, timestart)
        tmp_gaplength = sum_gaplength(report)
        
        better = (dst_gaplength is None) or (tmp_gaplength < dst_gaplength)
        
        if better:
            # Перезаписать старый файл и gapreport.
            if dst_gaplength:
                log.info(f'New file is better and will replace current.')
            shutil.move(tmp, dst)
            dst_gaplength = tmp_gaplength
        else:
            # Оставить старый файл и gapreport.
            report = json.load(open(gapfile))
            
        report['attempts'] = attempt
        json.dump(report, open(gapfile, 'w+'), indent=2)
            
        if tmp_gaplength == 0:
            # Нет разрывов
            return True
        
        if attempt < max_retries:
            wait = 1 * 10**(attempt-1)
            log.debug(f'Gap length {tmp_gaplength}. Retrying in {wait} seconds.')
            await asyncio.sleep(wait)
    else:
        log.debug(f'Last attempt failed, file has gaps.')
        return False
 

@command()
@option('--camid', required=True)
@option('--timestart', '-ts', required=True, type=click.DateTime(['%Y-%m-%dT%H:%M:%S%z']),
        help='Полное время формата 2018-03-18T07:45:00+06:00')
@option('--tmp-download-dir', type=click.Path(), required=True)
@option('--dst-download-dir', type=click.Path(), required=True)
@option('--logfile', type=click.Path())
@option('--loglevel', '-l', default='INFO',
        type=click.Choice(list(logging._nameToLevel), case_sensitive=False))
@option('--max-download-retries', type=int, default=2)
@option('--force/--no-force', '-f', default=False, help='Обнулить счетчик попыток и качать заново.')
def cli_download_segment(camid, timestart, dst_download_dir, tmp_download_dir, 
                         logfile, loglevel, **kw):
    """
    Скачать 15-минутный сегмент камеры camid, начинающийся со времени timestart во 
    временную папку, проверить на разрывы. 
    Если разрывов меньше чем в существующем файле, перезаписать его.
    Если есть разрывы - повторять скачивание max-download-retries раз.
    Сохранить отчет о разрывах в {segmentvideofile}.flv.gapreport.json
    Если force == False то продолжаем считать кол-во попыток с прошлого запуска.
    Если force == True то кол-во попыток с прошлого запуска обнуляется.
    """
    setup_logging(loglevel.upper(), logfile)
    
    tmpdir = Path(tmp_download_dir) / camid
    dstdir = Path(dst_download_dir) / camid
    
    if not exists(tmpdir): os.makedirs(tmpdir)
    if not exists(dstdir): os.makedirs(dstdir)
    
    result = asyncio.run(process_segment(
        camid,
        timestart,
        tmp = tmpdir / f'{camid}-{int(timestart.timestamp())}.flv',
        dst = dstdir / f'{camid}-{int(timestart.timestamp())}.flv',
        max_retries = kw['max_download_retries'],
        force = kw['force'],
    ))
    if result is False:
        sys.exit(1)  # Fail
    sys.exit(0)  # Success
    

if __name__ == '__main__':
    from environ import Env
    Env.read_env(str(Path(__file__).parent / '../env-local'))
    cli_download_segment(auto_envvar_prefix='CHURO')
    
