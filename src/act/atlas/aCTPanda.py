import cgi
import urllib2, urllib, socket, httplib
from threading import Thread
import pickle
import re
from act.common import aCTConfig

class PThr(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.arg = None
        self.func = None
    def run(self):
        self.func(self.arg)


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
            return None
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
        
    def getQueueStatus(self,cluster):
        node = {}
        query = "query?tpmes=pilotpars&queue=ARC-%s" % cluster
        urldata=self.__HTTPConnectMon__(query,node)
        status=None
        try:
            keys=urldata.split("|")
            for i in keys:
                if (i.find("status")) >= 0 :
                    res=re.match("status=(.+)",i)
                    status=res.group(1)
        except Exception,x:
            self.log.error(x)
            return None
        return status
        
    
    def getJob(self,siteName,prodSourceLabel=None):
        node={}
        node['siteName']=siteName
        if prodSourceLabel is not None:
            node['prodSourceLabel']=prodSourceLabel
        pid = None
        urldesc=None
        urldata=self.__HTTPConnect__('getJob',node)
        if not urldata:
            return (None,None)
        try:
            urldesc = cgi.parse_qs(urldata)
        except Exception,x:
            self.log.error(x)
            return (None,None)
        status = urldesc['StatusCode'][0]
        if status == '20':
            self.log.debug('No Panda activated jobs available')
            return (None, None)
        elif status == '0':
            pid = urldesc['PandaID'][0]
            self.log.info('New Panda job with ID %s' % pid)
        elif status == '60':
            self.log.error('Failed to contact Panda, proxy may have expired')             
        else:
            self.log.error('Check out what this Panda rc means %s' % status)
        self.log.debug("%s %s" % (pid,urldesc))
        return (pid,urldata)

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
        urldesc=None
        urldata=self.__HTTPConnect__('updateJob',node)
        try:
            urldesc = cgi.parse_qs(urldata)
        except Exception,x:
            self.log.error(x)
            return None
        return urldesc

    def queryJobInfo(self):
        node={}
        node['cloud']="ND"
        node['schedulerID']=self.conf.get(['panda','schedulerid'])
        urldesc=None
        try:
            urldata=self.__HTTPConnect__('queryJobInfoPerCloud',node)
        except:
            return []
        try:
            return pickle.loads(urldata)
        except:
            return []

    def getst(self,jids):
        thrs=[]
        i=0
        for j in jids:
            thrs.append(PThr())
            thrs[i].arg=j
            thrs[i].func=self.getStatus
            i=i+1
        for thr in thrs:
            thr.start()
        for thr in thrs:
            thr.join()
        
if __name__ == '__main__':
    # just for testing
    import logging
    import sys
    logging.basicConfig(level=logging.DEBUG)
    a=aCTPanda(logging.getLogger('test'))
    print a.getQueueStatus("pikolit.ijs.si")

    #a.getStatus(17058051)
    #a.updateStatus(17058796,'activated')
    #a.updateStatus(17058051,'activated')
    #a.updateStatus(17058797,'activated')
    #a.updateStatus(17397310,'activated')

    #a.getJob()

    #t = Thread(target=a.getStatus(17058051),name='st1')
    #t.setDaemon(True)
    #t.start()
    #t = Thread(target=a.getStatus(17058796),name='st2')
    #t.setDaemon(True)
    #t.start()
    #a.getst([17058051,17058796,17058797,17397310])
    l=a.queryJobInfo()
    for j in l:
        if j['jobStatus'] != 'activated':
            print j
    
