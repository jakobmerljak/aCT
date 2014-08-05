import os
import signal
import subprocess
import sys
import tempfile
import traceback
import aCTConfig
import aCTLogger
import aCTSignal
import aCTUtils
import aCTProcessManager
        
class aCTMain:
    """
    Main class to run aCT.
    """

    def __init__(self, args):

        # Check we have the right ARC version
        self.checkARC()

        # xml config file
        self.conf = aCTConfig.aCTConfigARC()
   
        # logger
        self.logger = aCTLogger.aCTLogger("aCTMain")
        self.log = self.logger()
        
        # daemon operations
        if len(args) >= 2:
            self.daemon(args[1])
         
        # process manager
        try:
            self.procmanager = aCTProcessManager.aCTProcessManager(self.log, self.conf)
        except Exception, e:
            self.log.critical("*** Unexpected exception! ***")
            self.log.critical(traceback.format_exc())
            self.log.critical("*** Process exiting ***")
            raise e
        
    def checkARC(self):
        """
        Check ARC can be used and is correct version
        """
        try:
            import arc
        except ImportError:
            print 'Error: failed to import ARC. Are ARC python bindings installed?'
            sys.exit(1)
            
        if arc.ARC_VERSION_MAJOR < 4:
            print 'Error: Found ARC version %s. aCT requires 4.0.0 or higher' % arc.ARC_VERSION
            sys.exit(1)

    def daemon(self, operation):
        """
        Start or stop process
        """
        pidfile = self.conf.get(['actlocation', 'pidfile'])
        pid = None
        try:
            with open(pidfile) as f:
                pid = f.read()
        except IOError:
            pass
        
        if operation == 'start':
            if pid:
                print "aCT already running (pid %s)" % pid
                sys.exit(1)
                
            # do double fork
            try:
                pid = os.fork()
                if pid > 0:
                    # exit first parent
                    sys.exit(0)
            except OSError, e:
                print "fork #1 failed: %d (%s)" % (e.errno, e.strerror)
                sys.exit(1)
        
            # decouple from parent environment
            os.setsid()
            os.umask(0)
        
            # do second fork
            try:
                pid = os.fork()
                if pid > 0:
                    # exit from second parent
                    sys.exit(0)
            except OSError, e:
                print "fork #2 failed: %d (%s)" % (e.errno, e.strerror)
                sys.exit(1)
        
            # redirect standard file descriptors
            sys.stdout.flush()
            sys.stderr.flush()
            si = open('/dev/null', 'r')
            so = open('/dev/null', 'a+')
            se = open('/dev/null', 'a+')
            os.dup2(si.fileno(), sys.stdin.fileno())
            os.dup2(so.fileno(), sys.stdout.fileno())
            os.dup2(se.fileno(), sys.stderr.fileno())

            # write pidfile
            with open(pidfile,'w+') as f:
                f.write(str(os.getpid()))
                
        elif operation == 'stop':
            if not pid:
                print 'aCT already stopped'
            else:
                try:
                    os.kill(int(pid), signal.SIGTERM)
                except OSError: # already stopped
                    pass
                os.remove(pidfile)
                print 'aCT stopped'
            sys.exit(0)
        else:
            print 'Usage: python aCTMain.py [start|stop]'
            sys.exit(1)
                
                
    def logrotate(self):
        """
        Run logrotate to rotate all logs
        """
        
        logrotateconf = '''
            %s/*.log {
                daily
                missingok
                rotate %s
                create
                nocompress
            }''' % (self.conf.get(["logger", "logdir"]), 
                    self.conf.get(["logger", "rotate"]))
        logrotatestatus = os.path.join(self.conf.get(["tmp", "dir"]), "logrotate.status")
        
        # Make a temp file with conf and call logrotate    
        with tempfile.NamedTemporaryFile() as temp:
            temp.write(logrotateconf)
            temp.flush()
            command = ['/usr/sbin/logrotate', '-s', logrotatestatus, temp.name]
            try:
                subprocess.call(command)
            except subprocess.CalledProcessError as e:
                self.log.warning("Failed to run logrotate: %s" % str(e))

                
    def run(self):
        """
        Main loop
        """
        try:
            self.log.info("Running")
            while 1:
                # Rotate logs
                self.logrotate()
                # check running processes are ok
                self.procmanager.checkRunning()
                # start and stop new processes as necessary
                self.procmanager.checkClusters()
                # sleep
                aCTUtils.sleep(10)

        except aCTSignal.ExceptInterrupt,x:
            pass
        except:
            self.log.critical("*** Unexpected exception! ***")
            self.log.critical(traceback.format_exc())
            self.log.critical("*** Process exiting ***")


    def finish(self):
        """
        clean finish handled by signals
        """
        self.log.info("Cleanup")

        
if __name__ == '__main__':
    am = aCTMain(sys.argv)
    am.run()
    am.finish()
