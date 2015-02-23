import os
import pickle

class aCTPandaJob:
    '''
    Class containing information on a panda job being processed by aCT. Normally
    at the end of the job this information is encoded in the pickle file produced
    by the pilot and downloaded by aCT. aCT adjusts certain information before
    sending it to panda. If the pickle is missing then default values are used. 
    The fields here are those expected by JobDispatcher.updateJob() in the Panda
    server.
    '''
    
    def __init__(self, jobinfo={}, filehandle=None, filename=''):
        '''
        Make a new aCTPandaJob. jobid and state are mandatory. jobinfo is a
        dictonary which can contain any of this class' attributes. If filehandle
        is given pickled info is loaded from the file object. If filename is
        given pickled info is read from a file.
        '''
        self.jobId = None
        self.state = None
        self.token = None
        self.transExitCode = None
        self.pilotErrorCode = None
        self.pilotErrorDiag = None
        self.timestamp = None
        self.timeout = 60
        self.xml = ''
        self.node = None
        self.workdir = None
        self.cpuConsumptionTime = None
        self.cpuConsumptionUnit = None
        self.remainingSpace = None
        self.schedulerID = None
        self.pilotID = None
        self.siteName = None
        self.messageLevel = None
        self.pilotLog = ''
        self.metaData = ''
        self.cpuConversionFactor = None
        self.exeErrorCode = None
        self.exeErrorDiag = None
        self.pilotTiming = None
        self.computingElement = None
        self.startTime = None
        self.endTime = None
        self.nEvents = None
        self.nInputFiles = None
        self.batchID = None
        self.attemptNr = None
        self.jobMetrics = None
        self.stdout = ''
        self.jobSubStatus=None
        
        if jobinfo:
            self.setAttributes(jobinfo)
        elif filehandle:
            self.setAttributes(pickle.load(filehandle))
        elif filename:
            with open(filename) as f:
                jobinfo = pickle.load(f)
                self.setAttributes(jobinfo)
    
    def setAttributes(self, jobinfo):
        '''
        Set attributes in the jobinfo dictionary
        '''
        for key, value in jobinfo.iteritems():
            if key in self.__dict__:
                self.__dict__[key] = value     

    def dictionary(self):
        '''
        Return a dictionary of all the attributes with set values
        '''
        return dict((k,v) for k,v in self.__dict__.iteritems() if v)
    
    def writeToFile(self, filename):
        '''
        Write a pickle of job info to filename. Overwrites an existing file.
        '''
        try:
            os.makedirs(os.path.dirname(filename), 0755)
        except:
            pass
        
        with open(filename, 'w') as f:
            pickle.dump(self.dictionary(), f)
            
if __name__ == '__main__':
    pj = aCTPandaJob(jobinfo={'jobId': 1234, 'state': 'running', 'batchID': 1234})  
    print pj.__dict__  
    print pj.dictionary()
    pj.writeToFile('/tmp/x/pickle')
    pj2 = aCTPandaJob(filename='/tmp/x/pickle')
    print pj2.dictionary()
    with open('/tmp/x/pickle') as f:
        pj3 = aCTPandaJob(filehandle=f)
        print pj3.dictionary()
        
