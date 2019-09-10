#!/usr/bin/env python
import asyncio
import json
import os
import shutil
from collections import defaultdict, namedtuple
from datetime import datetime
from os.path import exists, isdir, join, dirname

from app_state import state
    
try:
    from tools import sh, logger, setup_logging, sum_gaplength
except ImportError:
    from .tools import sh, logger, setup_logging, sum_gaplength


dummy = '''
#tb 0: 1/1000
0, 0, 0, 0, 39041, 23cbd52b74fd35b11d238537770fe5cc
0, 200, 200, 0, 1375, 58b545ce8693abf8ebcaae74cca19a93
'''

Frame = namedtuple('Frame', 'stream, dts, pts, duration, size, hash')

async def merge_files(files, dst):
    """ Склеить файлы, отрезая перекрывающиеся куски. """
    log = logger.get()
    input = []
    findhash = None
    outpoint = 0
    
    log.debug(f'Merging {dst}.')
    
    # Находим перекрывающиеся куски по одинаковым хэшам кадров.
    # TODO: do not reverse, calculate inpoint instead of outpoint.
    for file in reversed(files):
        cmd = 'ffmpeg -i %s -an -f framemd5 -c copy -' % file
        
        cmd = f'echo "{dummy}"'  # TODO: stub
        data = (await sh(cmd)).split(b'\n')
        
        timebase = [x for x in data if x.startswith(b'#tb')][0]
        tb_num, tb_den = timebase.split()[-1].split(b'/')
        
        frames = [x.replace(b',', b'').split() for x in data if x and not x.startswith(b'#')]
        
        for line in reversed(frames[1:]):
            frame = Frame(*line)
            if frame.hash == findhash:
                outpoint = float(frame.pts) * int(tb_num) / int(tb_den)
                break
            
        if outpoint:
            input.append('file %s\noutpoint %s' % (file, outpoint))
        else:
            input.append('file %s' % file)
        findhash = Frame(*frames[0]).hash
    
    # Склеиваем
    cmd = 'ffmpeg -nostats -hide_banner -avoid_negative_ts make_zero -fflags +genpts -f concat -safe 0' \
        ' -protocol_whitelist file,pipe -i - -c copy -flags +global_header -movflags +faststart -y '
    log.debug(cmd + str(dst))
    await sh(cmd + str(dst), '\n'.join(reversed(input)).encode('utf8'))


async def merge_camdir(srcdir, tmp, dst, force=False):
    """
    Проверить отчет о разрывах и склеить все файлы в папке в один временный файл.
    По окончании перенести временный файл в конечный.
    Вернуть True если файл успешно склеен.
    """
    log = logger.get()
    
    if dst.exists() and not force:
        log.debug('Склееный файл уже существует (требуется --force)')
        return
    
    # Проверим суммарное кол-во отсутствующих секунд с 8ч до 20ч допустимое для склеивания.
    interval = set('08:00 08:15 08:30 08:45 09:00 09:15 09:30 09:45 10:00 10:15 10:30 '
        '10:45 11:00 11:15 11:30 11:45 12:00 12:15 12:30 12:45 13:00 13:15 13:30 13:45 '
        '14:00 14:15 14:30 14:45 15:00 15:15 15:30 15:45 16:00 16:15 16:30 16:45 17:00 '
        '17:15 17:30 17:45 18:00 18:15 18:30 18:45 19:00 19:15 19:30 19:45'.split())
    gaplength_08_20 = 0  # Кол-во разрывов с 8ч до 20ч
    #gaplength_other = 0  # Кол-во разрывов в другое время
    unfinished_segments = {}   # Недокачанные сегменты с разрывами.
    for gapfile in sorted(srcdir.glob('*.gapreport.json')):
        try:
            report = json.load(open(gapfile))
        except:
            log.error(f'Malformed gapreport file {gapfile}')
            continue
        log.debug(f'Checking {gapfile}.')
        timestart = datetime.fromisoformat(report['localtime'])
        h, m = timestart.hour, timestart.minute
        if sum_gaplength(report) and report['attempts'] < state._config.max_download_retries:
            unfinished_segments[(h,m)] = gapfile
        if f'{h:02}:{m:02}' in interval:
            gaplength_08_20 += sum_gaplength(report)
            interval.discard(f'{h:02}:{m:02}')
        #else:
            #gaplength_other += sum_gaplength(report)
        
    for time in sorted(interval):
        log.error(f'Missing segment {time} in {srcdir}.')
        gaplength_08_20 += 900
            
    if gaplength_08_20 > state._config.merge_08_20_tolerate_gaps_duration:
        log.warning(f'{gaplength_08_20} отсутствующих секунд с 8ч до 20ч превышает допустимое для склеивания.')
        return False
    
    for time in unfinished_segments:
        log.warning(f'Сегмент {time} недокачан и содержит разрывы. {srcdir}')
        
    if unfinished_segments and not state._config.churo_merge_tolerate_incomplete_downloads:
        return False
    
    await merge_files(list(srcdir.glob('*.flv')), tmp)
    shutil.move(tmp, dst)
    return True
        
        
