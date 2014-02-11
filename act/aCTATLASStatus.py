# Handler for filling pandajobs information from arcjobs information. Also
# deals with post-processing of jobs and error handling.

import aCTSignal
import aCTUtils

class aCTATLASStatus:
    
    
    def run(self):
        """
        Main loop
        """        
        try:
            self.log.info("Running")
    
            while 1:
                self.conf.parse()
                
                # Check status of arcjobs
                # Query jobs in running state with tarcstate sooner than last run
                self.updateRunningJobs()
                # Query jobs in arcstate done, fix metadata.xml and update pandajobs
                # Set to toclean
                self.updateFinishedJobs()
                # Query jobs in arcstate failed, set to tofetch
                # Query jobs in arcstate donefailed, cancelled and lost, set to toclean.
                # If they should be resubmitted, set arcjobid to null in pandajobs
                # If not do post-processing and fill status in pandajobs
                self.updateFailedJobs()
                
                aCTUtils.sleep(100000)
    
        except aCTSignal.ExceptInterrupt,x:
            print x
            return

        
if __name__ == '__main__':
    aas=aCTATLASStatus()
    aas.run()