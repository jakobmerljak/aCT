import time
import os
import arc

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
    
    def __init__(self, name, cluster):
        
        self.name = name
        self.cluster = cluster
        
        # logger
        self.logger=aCTLogger.aCTLogger(self.name)
        self.log=self.logger()

        # config
        self.conf=aCTConfig.aCTConfig()
        # database
        self.db=aCTDBArc.aCTDBArc(self.log,self.conf.get(["db","file"]))
        
        # ARC Configuration
        self.uc = arc.UserConfig()
        self.uc.ProxyPath("/tmp/x509up_u%s" % os.getuid())
        self.uc.CACertificatesDirectory("/etc/grid-security/certificates")
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
                ip=int(self.conf.get(['periodicrestart', self.name]))
                if time.time()-self.starttime > ip and ip != 0 :
                    self.log.info("%s for %s exited for periodic restart", self.name, self.cluster)
                    return
        except aCTSignal.ExceptInterrupt,x:
            print x
            return

    def finish(self):
        '''
        Clean up code when process exits
        '''
        self.log.info("Cleanup for cluster %s", self.cluster)
