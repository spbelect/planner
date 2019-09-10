from asyncio import gather, get_running_loop
from app_state import state
from aiojobs._scheduler import Scheduler
from async_timeout import timeout
from loguru import logger


class Pool(Scheduler):
    """
    Сохраняет все таски в self.tasks, из которого таски автоматически не удаляются.
    """
    def __init__(self, limit):
        self.tasks = []
        super().__init__(loop=get_running_loop(), close_timeout=0, limit=limit, 
                         pending_limit=0, exception_handler=lambda *a: None)
        
    async def spawn(self, *a, **kw):
        job = await super().spawn(*a, **kw)
        self.tasks.append(job._do_wait(timeout=None))
        return job
    
    #def __del__(self):
        #print(1)
        #self._failed_task.cancel()
        #print(2)
        
    async def close(self):
        """
        Override deafult method to speedup closing.
        See [speedup bug] https://github.com/aio-libs/aiojobs/issues/100
        """
        if self._closed:
            return
        self._closed = True  # prevent adding new jobs
        if not self._jobs:
            return
        
        logger.warning(f'Closing {len(self._jobs)} jobs.')
        while not self._pending.empty():
            self._pending.get_nowait()
        await gather(*[self.close_job(x) for x in self._jobs], return_exceptions=True)
        self._jobs.clear()
        
    async def close_job(self, job):
        job._closed = True
        if job._task is None:
            # This will cause RuntimeWarning: coroutine 'coro' was never awaited, but its ok.
            # See [speedup bug] https://github.com/aio-libs/aiojobs/issues/100
            return
        if not job._task.done():
            job._task.cancel()
        try:
            with timeout(self._close_timeout, loop=self._loop):
                await job._task
        except asyncio.CancelledError:
            pass
        
    def _done(self, job):
        # Do not start next job if eventloop is stopping.
        # See [stoping feature] https://github.com/MagicStack/uvloop/issues/243
        if getattr(state, '_stopping', False):
            return
        return super()._done(job)
         
