import time
import os
import sys
import arc
import traceback

import aCTLogger
import aCTConfig
import aCTDBArc
import aCTUtils
import aCTSignal


class aCTProcess:
    '''
    Base class for all aCT processes. Sets up logging, configuration and ARC
    environment and provides basic start and stop functionality.
    '''
    
    def __init__(self):
        
        # Get agent name from /path/to/aCTAgent.py
        self.name = os.path.basename(sys.argv[0])[:-3]
        self.cluster = ''
        if len(sys.argv) == 2:
            self.cluster = sys.argv[1]
        
        # logger
        self.logger=aCTLogger.aCTLogger(self.name)
        self.log=self.logger()

        # config
        self.conf=aCTConfig.aCTConfigARC()
        # database
        self.db=aCTDBArc.aCTDBArc(self.log,self.conf.get(["db","file"]))
        
        # ARC Configuration
        # Credentials will be set by ARC agents for each job or set of jobs
        cred_type=arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)
        self.uc=arc.UserConfig(cred_type)
        self.uc.CACertificatesDirectory(str(self.conf.get(["voms", "cacertdir"])))
        timeout=int(self.conf.get(['atlasgiis','timeout']))
        self.uc.Timeout(timeout)

        # start time for periodic restart
        self.starttime=time.time()
        self.log.info("Started %s for cluster %s", self.name, self.cluster)


    def process(self):
        '''
        Called every loop during the main loop. Subclasses must implement this
        method with their specific operations.
        '''
        pass

    def run(self):
        '''
        Main loop
        '''
        try:
            while 1:
                # parse config file
                self.conf.parse()
                # Check if the site is in downtime
                if self.cluster not in self.conf.getList(['downtime', 'item']):
                    # do class-specific things
                    self.process()
                # sleep
                aCTUtils.sleep(1)
                # restart periodically for gsiftp crash
                ip=self.conf.get(['periodicrestart', self.name.lower()])
                if not ip:
                    continue
                ip = int(ip)
                if time.time()-self.starttime > ip and ip != 0 :
                    self.log.info("%s for %s exited for periodic restart", self.name, self.cluster)
                    return
        except aCTSignal.ExceptInterrupt,x:
            self.log.info("Received interrupt %s, exiting", str(x))
        except:
            self.log.critical("*** Unexpected exception! ***")
            self.log.critical(traceback.format_exc())
            self.log.critical("*** Process exiting ***")

    def finish(self):
        '''
        Clean up code when process exits
        '''
        self.log.info("Cleanup for cluster %s", self.cluster)
