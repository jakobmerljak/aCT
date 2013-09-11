import os
import aCTConfig
import aCTProxy
import aCTLogger
import aCTSignal
import aCTUtils
import aCTProcessManager
        
class aCTMain:
    """
    Main class to run aCT.
    """

    def __init__(self):

        # xml config file
        self.conf = aCTConfig.aCTConfigARC()
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
        self.logger = aCTLogger.aCTLogger("main")
        self.log = self.logger()
        self.log.info("start")
        
        # process manager
        self.procmanager = aCTProcessManager.aCTProcessManager(self.log, self.conf)

        # proxy extender
        self.proxy = aCTProxy.aCTProxy(Interval=3600)


    def run(self):
        """
        Main loop
        """
        try:
            self.log.info("Running")
            while 1:
                self.proxy.renew()
                # check running processes are ok
                self.procmanager.checkRunning()
                # start and stop new processes as necessary
                self.procmanager.checkClusters()
                # sleep
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
    am = aCTMain()
    am.run()
    am.finish()
