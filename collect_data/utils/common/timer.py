from datetime import datetime
import numpy as np
from collections import Counter
import operator

class Timer(dict):
    def __init__(self):
        self._now = datetime.now()
        self.total = 0
    def restart(self,):
        self._now = datetime.now()
    def stamp(self,key):
        if not key in self:
            self[key] = []
        duration = (datetime.now() - self._now).total_seconds()
        self.total += duration
        self[key].append(duration)
        self._now = datetime.now()
    def output(self):
        
        for k,secs in sorted(timer.items(),
                             key=lambda x: sum(x[1]),reverse=True):
            if sum(secs)/self.total < 0.01:
                continue
            print(k," (frac,total,n,mean,min,max) --> ",
                  sum(secs)/self.total,sum(secs),len(secs),
                  np.mean(secs),min(secs),max(secs))        
timer = Timer()
