import os
import time
from arclib import *
import aCTDBPanda
import aCTConfig
import re

class aCTOrphanJobs:
    
    def __init__(self):
        self.conf=aCTConfig.aCTConfig()
        self.db=aCTDBPanda.aCTDBPanda(None,self.conf.get(["db","file"]))


    def check(self):

        gisl=self.conf.getList(['atlasgiis','item'])
        timeout=int(self.conf.get(['atlasgiis','timeout']))
        atlasgiisl=[]
        for g in gisl:
            atlasgiisl.append(URL(str(g)))

        clusters = GetClusterResources(atlasgiisl,True,GetEffectiveSN(),timeout)
        jobs = GetAllJobs(clusters,True,GetEffectiveSN(),timeout)

        rows=self.db.getJobReport()

        actjobs=[]
        for r in rows:
            actjobs.append(r['arcjobid'])


        for j in jobs:
            if j.id in actjobs:
                continue
            #print j.id,j.job_name
            res = re.match('.+_tid(\d\d\d\d\d\d)._(\d\d\d\d\d).+',j.job_name)
            if res:
                #print res.group(1),res.group(2)
                print j.id, j.job_name

a=aCTOrphanJobs()
a.check()
