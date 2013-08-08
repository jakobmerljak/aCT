import subprocess
import sys

import aCTUtils
import aCTDBArc

class aCTProcessManager:
    '''
    Manager of aCT processes, starting and stopping as necessary
    '''
    
    def __init__(self, log, conf):
        
        # logger
        self.log = log
        # DB connection
        self.db = aCTDBArc.aCTDBArc(self.log, conf.get(["db","file"]))
        # list of processes to run per cluster
        self.processes = ['aCTSubmitter', 'aCTStatus', 'aCTFetcher', 'aCTCleaner']
        # dictionary of processes:aCTProcessHandler of which to run a single instance
        self.processes_single = {'aCTAutopilot': None}
        # dictionary of cluster to list of aCTProcessHandlers
        self.running = {}
        
    def checkRunning(self):
        '''
        Check for crashed processes and respawn
        '''
        for proc in [p for c in self.running for p in self.running[c]]:
            rc = proc.check()
            if rc == None :
                self.log.debug("process %s is running" % proc.name )
            else:
                self.log.info("restarting process %s" % proc.name )
                proc.restart()
                
        for (name, proc) in self.processes_single.items():
            if not proc:
                proc = self.aCTProcessHandler(name)
                proc.start()
                self.processes_single[name] = proc
            else:
                rc = proc.check()
                if rc == None :
                    self.log.debug("process %s is running" % name)
                else:
                    self.log.info("restarting process %s" % name)
                proc.restart()
                

    def checkClusters(self):
        '''
        Get the list of current clusters and start and kill necessary processes
        '''
        
        clusters = self.db.getActiveClusters()
        activeclusters = dict((k, v) for (k, v) in zip([c['cluster'] for c in clusters],
                                                       [c['COUNT(*)'] for c in clusters]))
        print activeclusters
        # First check for processes to kill
        for cluster in self.running:
            if cluster not in activeclusters:
                self.log.info("Stopping processes for %s", cluster)
                del self.running[cluster]
                    
        # Then check for new processes to start
        for cluster in activeclusters:
            if cluster not in self.running.keys():
                self.running[cluster] = []
                for proc in self.processes:
                    self.log.info("Starting process %s for %s", proc, cluster)
                    ph = self.aCTProcessHandler(proc, cluster)
                    ph.start()
                    self.running[cluster].append(ph)
                    
                
    class aCTProcessHandler:
        """
        Internal process control class wrapping Popen
        """
        def __init__(self, name, cluster=''):
            self.name = name
            self.cluster = cluster
            self.child = None
            self.fdout = open(name+".log","a")
            self.fderr = open(name+".err","a")
        def __del__(self):
            self.kill()
        def start(self):
            self.child = subprocess.Popen([sys.executable, self.name+".py", self.cluster], stdout=self.fdout, stderr=self.fderr)
        def check(self):
            return self.child.poll()
        def restart(self):
            if self.check() != None:
                self.start()
        def kill(self):
            if self.child:
                # first kill nicely (SIGTERM)
                self.child.terminate()
                aCTUtils.sleep(1)
                # make sure it is gone
                self.child.kill()

