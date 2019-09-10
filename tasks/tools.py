import sys
import contextvars
import logging
from asyncio import create_subprocess_shell, CancelledError
from asyncio.subprocess import PIPE
from pathlib import Path
from subprocess import CalledProcessError
from _io import TextIOWrapper

from app_state import state
import loguru
import psutil


varlogger = contextvars.ContextVar('logger', default=loguru.logger)

class logger:
    """ Convenient wrapper around varlogger. """
    @staticmethod
    def bind(**kw):
        log = varlogger.get().bind(**kw)
        varlogger.set(log)
        return log
    
    @staticmethod
    def get():
        return varlogger.get()


def tasklogsink(message):
    """ Loguru sink which writes each task log to its own file. """
    taskid = message.record["extra"].get("taskid")
    if taskid:
        logfile = Path(state._config.tasks_dir) / f'{taskid}.log'
        with open(logfile, 'a') as file:
            file.write(message)
            
            
class LoguruHandler(logging.Handler):
    """ Handler wich routes stdlib logging to loguru. """
    def emit(self, record):
        # Fixed depth has issue when logging.{method} used instead of logger.{method}
        # BUG: https://github.com/Delgan/loguru/issues/78
        logger_opt = logger.get().opt(depth=6, exception=record.exc_info)
        logger_opt.log(record.levelname, record.getMessage())


def formatter(record):
    format = '<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan>'
    if 'region' in record["extra"]:
        format += ' | <i>RU{extra[region]} UIK{extra[uik]}</i>'
    if 'camnum' in record["extra"]:
        format += ' <i>C{extra[camnum]}</i>'
    elif 'camid' in record["extra"]:
        format += ' | <i>{extra[camid]}</i>'
    if 'segment_time' in record["extra"]:
        format += ' <i>{extra[segment_time].hour:02}:{extra[segment_time].minute:02}</i>'
    return format + ' - <level>{message}</level>\n'
    
    
#def log_exception(f):
    #log = logger.get()
    #@log.catch
    #async def wrapper(*a, **kw):
        ##with :
        ##try:
        #return await f(*a, **kw)
        ##except Exception as e:
            ###log = logger.get().opt()
            ####print(str(e))
            ####with log.catch(Exception):
            ###log.exception(str(e))
            ##raise
    #return wrapper
    
def setup_logging(loglevel, filename=None):
    # Stdlib logging to /dev/null.
    logging.basicConfig(stream=open('/dev/null', 'w'), level=loglevel.upper())
    # Handler wich will route stdlib logging to loguru.
    logging.getLogger(None).addHandler(LoguruHandler())
    
    # Disable all loguru handlers.
    loguru.logger.configure(handlers=[])
    
    # Add stderr handler without backtrace pretty printing.
    # See issue re. flood of pretty traceback: https://github.com/Delgan/loguru/issues/77
    loguru.logger.add(sys.stderr, level=loglevel.upper(), format=formatter, backtrace=False)
    
    sys.tracebacklimit = 15
    
    if filename:
        # Single-file log.
        loguru.logger.add(filename, format=formatter, level=loglevel.upper())
    else:
        # Task logger which writes each task log to its own file.
        loguru.logger.add(tasklogsink, format=formatter, level=loglevel.upper())


async def sh(cmd, stdin=None, log_stdout=False, raise_error=True):
    """ Call shell command, return stdout. """
    log = logger.get().opt(depth=1)
    proc = await create_subprocess_shell(cmd, stdout=PIPE, stderr=PIPE, stdin=PIPE)
    try:
        stdout, stderr = await proc.communicate(stdin)
    except CancelledError:
        #if psutil.pid_exists(proc.pid):
            ## If Cancel was due to ctrl-c, normally child receives SIGINT before this
            ## parent code, and subprocess already dead at this point.
            #state._num_terminated = getattr(state, '_num_terminated', 0) + 1
            #proc.terminate()
        raise
    if proc.returncode == 0:
        if stdout and log_stdout:
            log.debug(stdout)
        return stdout
    else:
        if proc.returncode != -2:  # SIGINT sent when ctrl-c is pressed.
            message = f'"{cmd}" returned non-zero exit status {proc.returncode}'
            if stdout:
                message += f'\n{stdout.decode("utf8")}'
            if stderr:
                message += f'\n{stderr.decode("utf8")}'
            log.error(message)
        if raise_error:
            raise CalledProcessError(proc.returncode, cmd, stdout, stderr)
        else:
            return False


def sum_gaplength(gapreport):
    """ Суммарная длительность разрывов в репорте """
    if 'invalid_file' in gapreport:
        return 900
    
    duration_gap = gapreport.get('duration_error', 0) 
    diff_gaps = sum(x['len'] for x in gapreport.get('maxdiff_errors', []))
    return diff_gaps + duration_gap
    
