#import asyncio
#from functools import partial
#from concurrent.futures import ThreadPoolExecutor, as_completed

#class Pool(ThreadPoolExecutor):
    #def __init__(self, *a, **kw):
        #self.tasks = []
        #super().__init__(*a, **kw)
        
    #def run(self, task, *args, **kw):
        #result = asyncio.get_running_loop().run_in_executor(self, partial(task, *args, **kw))
        #self.tasks.append(result)
        #return result
        
