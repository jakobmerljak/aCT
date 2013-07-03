import os
import time
import aCTDBPanda
from arclib import *
import cgi
import arclfc
import lfcthr as lfc
import LFCTools
import aCTConfig
import aCTLogger
import aCTSignal
import aCTUtils
from threading import Thread,Lock


class aCTResolver:

    def __init__(self):
        self.logger=aCTLogger.aCTLogger("resolver")
        self.log=self.logger()

        self.conf=aCTConfig.aCTConfig()
        self.db=aCTDBPanda.aCTDBPanda(self.log,self.conf.get(["db","file"]))

        self.log.info("Started")

        # start time for periodic restart
        self.starttime=time.time()

    def GetLFNs_old(self,guids):
        """
        Lookup for LFNs in LFC by guid. Bulk method
        """
        host=str(self.conf.get(['lfc','host'])).split("/")[2]
        os.environ['LFC_HOST'] = host
        lfcurl=self.conf.get(['lfc','host'])
        if lfc.lfc_startsess(host, '') != 0:
            return guids
        for g in guids.keys():
            try:
                res=arclfc.getlfn(str(g))
            except Exception,x:
                self.log.error(x) 
                lfc.lfc_endsess()
                return guids
            if res is None:
                guids[g]=None
            else:
                guids[g]=lfcurl+res
        lfc.lfc_endsess()
        return guids

    def GetLFNs(self,guids):
        """
        Lookup for LFNs in LFC by guid. Bulk method
        """
        #host=str(self.conf.get(['lfc','host'])).split("/")[2]
        #os.environ['LFC_HOST'] = host
        lfcurl=self.conf.get(['lfc','host'])
        #if lfc.lfc_startsess(host, '') != 0:
        #    return guids
        for g in guids.keys():
            try:
                #res=arclfc.getlfn(str(g))
		res=":guid="+str(g)
            except Exception,x:
                self.log.error(x) 
                #lfc.lfc_endsess()
                return guids
            if res is None:
                guids[g]=None
            else:
                guids[g]=lfcurl+res
        #lfc.lfc_endsess()
        return guids

    def guidsTolfns(self):
        """
        translate input guids to lfns
        """
        jobs=self.db.getJobs("pstatus='sent' and trfstatus='tolfclfns' limit 200")
        if len(jobs) == 0:
            return 0
        self.log.info("Resolving lfns %d jobs:" % len(jobs))
        guids={}
        for j in jobs:
            jobdesc = cgi.parse_qs(j['pandajob'])
            if jobdesc.has_key('GUID'):
              for i in jobdesc['GUID'][0].split(","):
                  guids[i]=None

        lguids=self.GetLFNs(guids)
        self.log.info("Resolved %d lfns:" % len(lguids))

        for j in jobs:
            lfns=[]
            jobdesc = cgi.parse_qs(j['pandajob'])
            ok=True
            if jobdesc.has_key('GUID'):
              for i in jobdesc['GUID'][0].split(","):
                  if lguids[i] is None:
                      ok=False
		  else:
                      lfns.append(i+"="+lguids[i])
              if not ok:
                  continue
            dlfns=",".join(lfns)
            #print dlfns
            n={}
            n['trfstatus']='tosubmit'
            n['lfns']=dlfns
            self.db.updateJob(j['pandaid'],n)

    def run(self):
        """
        Main loop
        """
        try:
            while 1:
                # parse config file
                self.conf.parse()
                # translate input guids
                self.guidsTolfns()
                aCTUtils.sleep(10)
                # restart periodicaly for gsiftp crash
                ip=int(self.conf.get(['periodicrestart','resolver']))
                if time.time()-self.starttime > ip and ip != 0 :
                    self.log.info("Resolver exited for periodic restart")
                    return
        except aCTSignal.ExceptInterrupt,x:
            print x
            return

    def finish(self):
        self.log.info("Cleanup")

# Main
if __name__ == '__main__':
    asb=aCTResolver()
    asb.run()
    asb.finish()
    
