import cgi
import json
import urllib2, urllib, socket, httplib
import os
import pickle
import re
from act.common import aCTConfig

class aCTPanda:


    def __init__(self,logger, proxyfile):
        self.conf=aCTConfig.aCTConfigATLAS()
        #self.srv='https://pandasrv.usatlas.bnl.gov:25443/server/panda/'
        #self.siteName='ARC'
        self.srv=self.conf.get(['panda','server'])
        self.mon=self.conf.get(['panda','monitor'])
        res=re.match('.+//([^/]+)(.+)',self.srv)
        self.hostport=res.group(1)
        self.topdir=res.group(2)
        #self.proxypath=self.conf.get(['voms','proxypath'])
        #self.proxypath='/tmp/x509up_u%s' % str(os.getuid())
        self.proxypath=proxyfile
        #self.siteName=self.conf.get(['panda','site'])
        self.log=logger
        # timeout in seconds
        # TODO checkw
        self.timeout = int(self.conf.get(['panda','timeout']))
        socket.setdefaulttimeout(self.timeout)
        
    def __HTTPConnect__(self,mode,node):
        urldata = None
        try:
            conn = httplib.HTTPSConnection(self.hostport, key_file=self.proxypath, cert_file=self.proxypath )
            rdata=urllib.urlencode(node)
            #req=urllib2.Request(self.srv+mode)
            conn.request("POST", self.topdir+mode,rdata)
            resp = conn.getresponse()
            urldata = resp.read()
            conn.close()
        except Exception,x:
            self.log.error("error in connection: %s" %x)
        return urldata

    def __HTTPConnectMon__(self,mode,node):
        urldata = None
        try:
            rdata=urllib.urlencode(node)
            req=urllib2.Request(self.mon+mode)
            fd=urllib2.urlopen(req,rdata)
            urldata = fd.read()
            fd.close
        except Exception,x:
            self.log.error("error in connection: %s" %x)
            return None
        return urldata
        
    def getQueueStatus(self, queue=None):
        node = {}
        if queue:
            node = {'site': queue}
        self.log.debug('Getting queue info')
        urldata = self.__HTTPConnect__('getJobStatisticsWithLabel', node)
        if not urldata:
            self.log.warning('No queue info returned by panda')
            return None

        try:
            data = pickle.loads(urldata)
        except Exception as e:
            self.log.error('Could not load panda response: %s' % urldata)
            return None

        return data
    
    def getJob(self,siteName,prodSourceLabel=None,getEventRanges=True):
        node={}
        node['siteName']=siteName
        if prodSourceLabel is not None:
            node['prodSourceLabel']=prodSourceLabel
        pid = None
        urldesc=None
        eventranges=None
        self.log.debug('Fetching jobs for %s %s' % ( siteName, prodSourceLabel) )
        urldata=self.__HTTPConnect__('getJob',node)
        if not urldata:
            self.log.info('No job from panda')
            return (None,None,None,None)
        try:
            urldesc = cgi.parse_qs(urldata)
        except Exception,x:
            self.log.error(x)
            return (None,None,None,None)
        status = urldesc['StatusCode'][0]
        if status == '20':
            self.log.debug('No Panda activated jobs available')
            return (-1,None,None,None)
        elif status == '0':
            pid = urldesc['PandaID'][0]
            self.log.info('New Panda job with ID %s' % pid)
            if getEventRanges and 'eventService' in urldesc and urldesc['eventService'][0] == 'True':
                node = {}
                node['pandaID'] = urldesc['PandaID'][0]
                node['jobsetID'] = urldesc['jobsetID'][0]
                node['taskID'] = urldesc['taskID'][0] 
                node['nRanges'] = 500 # TODO: configurable?
                if siteName == 'BOINC-ES':
                    node['nRanges'] = 100
                eventranges = self.getEventRanges(node)
        elif status == '60':
            self.log.error('Failed to contact Panda, proxy may have expired')             
        else:
            self.log.error('Check out what this Panda rc means %s' % status)
        self.log.debug("%s %s" % (pid,urldesc))
        return (pid,urldata,eventranges,prodSourceLabel)

    def getEventRanges(self, node):
        self.log.debug('%s: Fetching event ranges' % node['pandaID'])
        urldata=self.__HTTPConnect__('getEventRanges', node)
        if not urldata:
            self.log.info('%s: Could not get event ranges from panda' % node['pandaID'])
            return None
        try:
            urldesc = cgi.parse_qs(urldata)
        except Exception,x:
            self.log.error(x)
            return None
        self.log.debug('%s: Panda returned %s' % (node['pandaID'], urldesc))
        status = urldesc['StatusCode'][0]
        if status == '0':
            return urldesc['eventRanges'][0]
        if status == '60':
            self.log.error('Failed to contact Panda, proxy may have expired')             
        else:
            self.log.error('Check out what this Panda rc means %s' % status)
        return None

    def updateEventRange(self, node):
        self.log.debug('Updating event range %s: %s' % (node['eventRangeID'], str(node)))
        urldata=self.__HTTPConnect__('updateEventRange', node)
        self.log.debug('panda returned %s' % str(urldata))
        if not urldata:
            self.log.info('Could not update event ranges in panda')
            return None
        try:
            urldesc = cgi.parse_qs(urldata)
        except Exception,x:
            self.log.error(x)
            return None
        return urldesc

    def updateEventRanges(self, node):
        urldata=self.__HTTPConnect__('updateEventRanges', node)
        self.log.debug('panda returned %s' % str(urldata))
        if not urldata:
            self.log.info('Could not update event ranges in panda')
            return None
        try:
            urldesc = cgi.parse_qs(urldata)
        except Exception,x:
            self.log.error(x)
            return None
        return urldesc

    def getStatus(self,pandaId):
        self.log.info("entry %d" % pandaId)
        node={}
        node['ids']=pandaId
        urldesc=None
        urldata=self.__HTTPConnect__('getStatus',node)
        try:
            urldesc = cgi.parse_qs(urldata)
        except Exception,x:
            self.log.error(x)
            return None
        return urldesc
        

    def updateStatus(self,pandaId,state,desc={}):
        node={}
        node['jobId']=pandaId
        node['state']=state
        node['schedulerID']=self.conf.get(['panda','schedulerid'])
        if desc:
            for key in desc.keys():
                node[key]=desc[key]
        # protection against bad pickles
        if 'jobId' not in node or not node['jobId']:
            node['jobId'] = pandaId
        if 'state' not in node or not node['state']:
            node['state'] = state
        urldesc=None
        urldata=self.__HTTPConnect__('updateJob',node)
        #self.log.debug('panda returned %s' % str(urldata))
        try:
            urldesc = cgi.parse_qs(urldata)
        except Exception,x:
            self.log.error(x)
            return None
        return urldesc

    def updateStatuses(self, jobs):
        # Caller must make sure jobId and state are defined for each job
        jobdata = []
        for job in jobs:
            node = job
            node['schedulerID'] = self.conf.get(['panda','schedulerid'])
            jobdata.append(node)
        urldata=self.__HTTPConnect__('updateJobsInBulk', {'jobList': json.dumps(jobdata)})
        try:
            urldesc = json.loads(urldata)
        except Exception,x:
            self.log.error(x)
            return {}
        return urldesc


    def queryJobInfo(self, cloud='ND'):
        node={}
        node['cloud']=cloud
        node['schedulerID']=self.conf.get(['panda','schedulerid'])
        try:
            urldata=self.__HTTPConnect__('queryJobInfoPerCloud',node)
        except:
            return []
        try:
            return pickle.loads(urldata)
        except:
            return []


if __name__ == '__main__':
    
    from act.common.aCTLogger import aCTLogger
    logger = aCTLogger('test')
    log = logger()
    p = aCTPanda(log, os.environ['X509_USER_PROXY'])
    print p.getQueueStatus('UIO')
