import subprocess
import os

import aCTUtils
from act.arc import aCTDBArc

class aCTProcessManager:
    '''
    Manager of aCT processes, starting and stopping as necessary
    '''
    
    def __init__(self, log, conf):
        
        # logger
        self.log = log
        self.actlocation = conf.get(["actlocation","dir"])
        self.logdir = conf.get(["logger", "logdir"])
        # DB connection
        self.db = aCTDBArc.aCTDBArc(self.log, conf.get(["db","file"]))
        # list of processes to run per cluster
        self.processes = ['act/arc/aCTSubmitter', 'act/arc/aCTStatus', 'act/arc/aCTFetcher', 'act/arc/aCTCleaner']
        # dictionary of processes:aCTProcessHandler of which to run a single instance
        # TODO: app-specific processes in conf file instead of hard-coded
        self.processes_single = {'act/atlas/aCTAutopilot':None, 
                                 'act/atlas/aCTPanda2Arc':None,
                                 'act/common/aCTProxyHandler':None,
                                 'act/atlas/aCTATLASStatus':None,
                                 'act/atlas/aCTValidator':None,
                                 'act/atlas/aCTAGISFetcher':None
                                 }
        # dictionary of cluster to list of aCTProcessHandlers
        self.running = {}
        # dictionary of cluster to Submitter processes handlers, there should
        # be one per unique cluster in clusterlist
        self.submitters = {}
        
        # Start single instance processes
        for process in self.processes_single:
            proc = self.aCTProcessHandler(process, self.logdir, actlocation=self.actlocation)
            proc.start()
            self.processes_single[process] = proc
        
    def checkRunning(self):
        '''
        Check for crashed processes and respawn
        '''
        
        # All running per-cluster processes
        procs = [p for c in self.running for p in self.running[c]]
        # Submitter processes
        procs.extend(self.submitters.values())
        # Single instance processes
        procs.extend(self.processes_single.values())
        
        for proc in procs:
            rc = proc.check()
            if rc == None :
                self.log.debug("process %s for %s is running", proc.name, proc.cluster )
            else:
                self.log.info("restarting process %s for %s", proc.name, proc.cluster )
                proc.restart()

    def checkClusters(self):
        '''
        Get the list of current clusters and start and kill necessary processes
        '''
        
        clusters = self.db.getActiveClusters()
        activeclusters = dict((k, v) for (k, v) in zip([c['cluster'] for c in clusters],
                                                       [c['COUNT(*)'] for c in clusters]))
        clusters = self.db.getClusterLists()
        clusterlists = dict((k, v) for (k, v) in zip([c['clusterlist'] for c in clusters],
                                                     [c['COUNT(*)'] for c in clusters]))
        
        clusterlist = [] # unique list of clusters in lists
        for cluster in clusterlists:
            if not cluster:
                cluster = ''
            clist = cluster.split(',')
            for c in clist:
                if c not in clusterlist:
                    self.log.info("add cluster %s", c)
                    clusterlist.append(c)

        # First check for processes to kill
        for cluster in self.running.keys():
            if cluster not in activeclusters:
                self.log.info("Stopping processes for %s", cluster)
                del self.running[cluster]

        # Stop submitters no longer needed
        for cluster in self.submitters.keys():
            if cluster not in clusterlist:
                self.log.info("Stopping aCTSubmitter for %s", cluster)
                del self.submitters[cluster]
                    
        # Check for new processes to start
        for cluster in activeclusters:
            if not cluster: # Job not submitted yet
                continue
            if cluster not in self.running.keys():
                # If submitter was already started, kill it and start again here
                if cluster in self.submitters:
                    self.log.info("Stopping aCTSubmitter for %s", cluster)
                    del self.submitters[cluster]
                
                self.running[cluster] = []
                for proc in self.processes:
                    self.log.info("Starting process %s for %s", proc, cluster)
                    ph = self.aCTProcessHandler(proc, self.logdir, cluster, actlocation=self.actlocation)
                    ph.start()
                    self.running[cluster].append(ph)
            
        # Start any new submitters required
        for cluster in clusterlist:
            if cluster not in self.submitters and cluster not in self.running:
                self.log.info("Starting process aCTSubmitter for %s", cluster)
                ph = self.aCTProcessHandler('act/arc/aCTSubmitter', self.logdir, cluster, actlocation=self.actlocation)
                ph.start()
                self.submitters[cluster] = ph
        
                
    class aCTProcessHandler:
        """
        Internal process control class wrapping Popen
        """
        def __init__(self, name, logdir, cluster='', actlocation=''):
            self.name = name
            self.cluster = cluster
            self.child = None
            self.actlocation = actlocation
            # Redirect stdout and stderr to process log
            self.fdout = open(os.path.join(logdir, name[name.rfind('/')+1:]+'.log'), 'a')            
        def __del__(self):
            self.kill()
        def start(self):
            self.child = subprocess.Popen(['/usr/bin/python2', os.path.join(self.actlocation, self.name+".py"), self.cluster], stdout=self.fdout, stderr=subprocess.STDOUT)            
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

