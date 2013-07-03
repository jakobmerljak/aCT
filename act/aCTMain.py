import os
import subprocess
import time
import sys
import aCTConfig
import aCTProxy
import aCTLogger
import sys
from threading import Thread
import aCTSignal
import aCTUtils
import pickle
import re


class aCTProcess:
    """
    Process control class to spawn aCTSubmitter and aCTDownloader. 
    """
    def __init__(self,name):
        self.name=name
        self.child=None
        self.fdout=open(name+".log","a")
        self.fderr=open(name+".err","a")
    def start(self):
        self.child=subprocess.Popen([sys.executable,self.name+".py"],stdout=self.fdout,stderr=self.fderr)
    def check(self):
        return self.child.poll()
    def restart(self):
        if self.check() != None:
            self.start()
        
class aCTMain:

    """
    Main class to run aCT. Three major functions: init, run, finish
    """

    def __init__(self):

        # xml config file
        self.conf=aCTConfig.aCTConfig()
        # create log dirs
        try:
           os.mkdir(self.conf.get(["tmp","dir"]))
        except:
           pass
        try:
           os.mkdir(str(self.conf.get(["tmp","dir"]))+"/log")
        except:
           pass
        # logger
        self.logger=aCTLogger.aCTLogger("main")
        self.log=self.logger()
        self.log.info("start")

        # dictionary with process descriptions
        self.procs={}

        self.procs['aCTAutopilot']={}
        self.procs['aCTAutopilot']['program']=aCTProcess('aCTAutopilot')
        self.procs['aCTSubmitter']={}
        self.procs['aCTSubmitter']['program']=aCTProcess('aCTSubmitter')
        self.procs['aCTResolver']={}
        self.procs['aCTResolver']['program']=aCTProcess('aCTResolver')
        self.procs['aCTDownloader']={}
        self.procs['aCTDownloader']['program']=aCTProcess('aCTDownloader')
        self.procs['aCTCleaner']={}
        self.procs['aCTCleaner']['program']=aCTProcess('aCTCleaner')

        # proxy extender
        self.proxy=aCTProxy.aCTProxy(Interval=3600)
        

    def checkProcs(self):
        # check for crashed processes and respawn
        for p in self.procs.keys():
            pr=self.procs[p]['program']
            rc=pr.check()
            #print rc
            if rc == None :
               pass
               self.log.debug("process %s is running" % p )
            else:
               self.log.info("restarting process %s" % p )
               pr.restart()
               pass

            
    def init(self):
        """
        Init the ACT. Start the subprocesses aCTDownloader, aCTSubmitter
        """
        self.log.info("init")
        for p in self.procs.keys():
            pr=self.procs[p]['program']
            pr.start()


    def run(self):
        """
        Main loop
        """
        try:
           self.log.info("Running")
           self.checkProcs()

           while 1:
              # try to reparse config file if it was modified. 
              self.conf.parse()
              self.proxy.renew()
              # check processes
              self.checkProcs()
              # 
              aCTUtils.sleep(10)

        except aCTSignal.ExceptInterrupt,x:
           print x
           return
            

    def finish(self):
        """
        clean finish handled by signals
        """
        self.log.info("Cleanup")


        
if __name__ == '__main__':
    am=aCTMain()
    am.init()
    am.run()
    am.finish()
