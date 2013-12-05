import re
import os
import time
import arc
from aCTDB import aCTDB
import aCTConfig

class aCTDBArc(aCTDB):
    
    def __init__(self,logger,dbname="aCTjobs.db"):
        aCTDB.__init__(self, logger, dbname)

        conf = aCTConfig.aCTConfigARC()
        self.proxydir=conf.get(["voms","proxystoredir"])
        if not os.path.isdir(self.proxydir):
            os.mkdir(self.proxydir)
                
        # mapping from Job class attribute types to column types
        self.jobattrmap = {int: 'integer',
                      str: 'varchar(255)',
                      arc.JobState: 'varchar(255)',
                      arc.StringList: 'text',
                      arc.URL: 'text',
                      arc.Period: 'int',
                      arc.Time: 'datetime',
                      arc.StringStringMap:'text'}
        ignoremems=['STDIN',
                    'STDOUT',
                    'STDERR',
                    'STAGEINDIR',
                    'STAGEOUTDIR',
                    'SESSIONDIR',
                    'JOBLOG',
                    'JOBDESCRIPTION']
        
        # Attributes of Job class mapped to DB column type
        self.jobattrs={}
        j=arc.Job()
        for i in dir(j):
            if re.match('^__',i):
                continue
            if i in ignoremems:
                continue
            if type(getattr(j, i)) in self.jobattrmap:
                self.jobattrs[i] = type(getattr(j, i))
        

    def createTables(self):
        '''
        arcjobs: columns are attributes of arc.Job plus the following:
          - id:
          - created: timestamp of creation of the record
          - modified: timestamp of last record update
          - arcstate: tosubmit, submitting, submitted, running, stalled, tocancel,
                      cancelling, cancelled, finished, failed, tofetch, torerun,
                      toresubmit, done, donefailed, lost, toclean
            "to" states are set by application engine or ARC engine for retries
          - tarcstate: time stamp of last arcstate
          - cluster: hostname of the cluster chosen for the job
          - clusterlist: comma separated list of clusters on which the job may
            run. Can be empty.
          - jobdesc: job description added by the application engine
          - attemptsleft: Number of attempts left to run the job
          - downloadfiles: Comma-separated list of specific files to download
            after job finished. If empty download all in job desc.
          - rerunnable:
          - proxyid: id of corresponding proxies entry of proxy to use for this job
        proxies: columns are the following:
          - id:
          - proxy:
          - proxypath: path to file containing the proxy
          - dn: dn of the proxy
          - role: role of the proxy
          - myproxyid: id from myproxy
          - expirytime: timestamp for when proxy is expiring
        '''
        aCTDB.createTables(self)
        # in MySQL the first timestamp specified gets automatically updated to
        # current time for each change. 
        create="""CREATE TABLE arcjobs (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            modified TIMESTAMP,
            created TIMESTAMP,
            arcstate VARCHAR(255),
            tarcstate TIMESTAMP,
            cluster TEXT,
            clusterlist TEXT,
            jobdesc TEXT,
            attemptsleft INTEGER,
            rerunable TEXT,
            downloadfiles TEXT,
            proxyid INTEGER,
            """+",".join(['%s %s' % (k, self.jobattrmap[v]) for k, v in self.jobattrs.items()])+")"
        c=self.getCursor()
        try:
            c.execute("drop table arcjobs")
        except:
            self.log.warning("no arcjobs table")
        try:
            c.execute(create)
            self.conn.commit()
        except Exception,x:
            self.log.error("failed create table %s" %x)
        self.log.info("creating proxies table")
        create="""CREATE TABLE proxies (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            proxy BLOB,
            expirytime DATETIME,
            proxypath TEXT,
            dn VARCHAR(255),
            role VARCHAR(255),
            myproxyid VARCHAR(255) )"""
        try:
            c.execute("drop table proxies")
        except:
            self.log.warning("no proxies table")
        try:
            c.execute(create)
            self.conn.commit()
        except Exception,x:
            self.log.error("failed create table %s" %x)

    def Commit(self):
        '''
        Commit a transaction after calling a Lazy method.
        '''
        self.conn.commit()

    def insertArcJob(self, job):
        '''
        Add new arc Job object. Only used for testing and recreating db.
        '''
        c=self.getCursor()
        j = self._job2db(job)
        c.execute("insert into arcjobs (modified,created,"+",".join(j.keys())+") values ('"+str(self.getTimeStamp())+"','"+str(self.getTimeStamp())+"','"+"','".join(j.values())+"')")
        c.execute("SELECT LAST_INSERT_ID()")
        row = c.fetchone()
        self.conn.commit()
        return row
        
    def insertArcJobDescription(self, jobdesc, proxyid='', maxattempts=0, clusterlist=''):
        '''
        Add a new job description for the ARC engine to process. If specified
        the job will be sent to a cluster in the given list.
        '''
        # todo: find some usefull default for proxyid
        c=self.getCursor()
        desc = {}
        desc['created'] = self.getTimeStamp()
        desc['modified'] = desc['created']
        desc['arcstate'] = "tosubmit"
        desc['tarcstate']  = desc['created']
        desc['cluster']  = ''
        desc['clusterlist'] = clusterlist
        desc['jobdesc'] = jobdesc
        desc['attemptsleft'] = maxattempts
        desc['proxyid'] = proxyid
        s="insert into arcjobs" + " ( " + ",".join(['%s' % (k) for k in desc.keys()]) + " ) " + " values " + \
            " ( " + ",".join(['%s' % (k) for k in ["%s"] * len(desc.keys()) ]) + " ) "
        c.execute(s,desc.values())
        c.execute("SELECT LAST_INSERT_ID()")
        row = c.fetchone()
        self.conn.commit()
        return row
        

    def deleteArcJob(self, id):
        '''
        Delete job from ARC table.
        '''
        c=self.getCursor()
        c.execute("delete from arcjobs where id="+str(id))
        self.conn.commit()

    def updateArcJob(self, id, desc, job=None):
        '''
        Update arc job fields specified in desc and fields represented by arc
        Job if job is specified.
        '''
        self.updateArcJobLazy(id, desc, job)
        self.conn.commit()

    def updateArcJobLazy(self, id, desc, job=None):
        '''
        Update arc job fields specified in desc and fields represented by arc
        Job if job is specified. Does not commit after executing update.
        '''
        desc['modified']=self.getTimeStamp()
        s = "update arcjobs set " + ",".join(['%s=%%s' % (k) for k in desc.keys()])
        if job:
            s += "," + ",".join(['%s=%%s' % (k) for k in self._job2db(job).keys()])
        s+=" where id="+str(id)
        c=self.getCursor()
        c.execute("select id from arcjobs where id="+str(id))
        row=c.fetchone()
        if row is None:
            self.insertArcJob(id)
        if job:
            c.execute(s,desc.values() + self._job2db(job).values() )
        else:
            c.execute(s,desc.values())

    def getArcJobInfo(self,id,columns=[]):
        '''
        Return a dictionary of column name: value for the given id and columns
        ''' 
        c=self.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM arcjobs WHERE id="+str(id))
        row=c.fetchone()
        # mysql SELECT returns list, we want dict
        if not isinstance(row,dict):
            row=dict(zip([col[0] for col in c.description], row))
        return row

    def getArcJob(self, id):
        '''
        Return a dictionary of {proxyid: {id: arc.Job, id: arc.Job}}.
        '''
        c=self.getCursor()
        c.execute("SELECT "+",".join(self.jobattrs.keys())+" FROM arcjobs WHERE id="+str(id))
        row = c.fetchone()
        if not isinstance(row,dict):
            row=dict(zip([col[0] for col in c.description], row))
        return {id: self._db2job(row)}
        
    def getArcJobsInfo(self, select, columns=[], lock=False):
        '''
        Return a list of column: value dictionaries for jobs matching select.
        If lock is True the row will be locked if possible.
        '''
        if lock:
            select += self.addLock()
        c=self.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM arcjobs WHERE "+select)
        rows=c.fetchall()
        return rows

    def getArcJobs(self,select):
        '''
        Return a dictionary of id: arc.Job for jobs matching select
        '''
        c=self.getCursor()
        c.execute("SELECT id, proxyid, "+",".join(self.jobattrs.keys())+" FROM arcjobs WHERE "+select)
        rows=c.fetchall()
        d = {}
        if isinstance(rows, tuple):
            rows=dict(zip([col[0] for col in c.description], zip(*[list(row) for row in rows])))
            for row in rows:
                d[row[0]] = self._db2job(dict(zip([col[0] for col in c.description], row[1:])))
        # sqlite returns list of dictionaries
        if isinstance(rows, list):
            for row in rows:
                if not row['proxyid'] in d:
                    d[row['proxyid']] = {}
                d[row['proxyid']][row['id']] = self._db2job(row)
            
        return d
    
    def getNArcJobs(self):
        '''
        Return the total number of jobs in the table
        '''
        c=self.getCursor()
        c.execute("SELECT COUNT(*) FROM arcjobs")
        row = c.fetchone()
        return row['COUNT(*)']
         
    def getActiveClusters(self):
        '''
        Return a list and count of clusters
        '''
        c=self.getCursor()
        c.execute("SELECT cluster, COUNT(*) FROM arcjobs GROUP BY cluster")
        rows=c.fetchall()
        print "getActiveClusters", rows
        return rows
    
    def getClusterLists(self):
        '''
        Return a list and count of clusterlists for jobs to submit
        '''
        c=self.getCursor()
        # submitting state is included here so that a submitter process is not
        # killed while submitting jobs
        c.execute("SELECT clusterlist, COUNT(*) FROM arcjobs WHERE arcstate='tosubmit' OR arcstate='submitting' GROUP BY clusterlist")
        rows=c.fetchall()
        print "getClusterLists", rows
        return rows
    
    def _db2job(self, dbinfo):
        '''
        Convert a dictionary of DB key value into arc Job object
        '''
        j = arc.Job()
        for attr in self.jobattrs:
            if attr not in dbinfo or dbinfo[attr] is None:
                continue
            # Some object types need special treatment
            if self.jobattrs[attr] == arc.StringList:
                l = arc.StringList()
                for item in str(dbinfo[attr]).split('|'):
                    l.append(item)
                setattr(j, attr, l)
                continue
            if self.jobattrs[attr] == arc.StringStringMap:
                m = arc.StringStringMap()
                d = eval(dbinfo[attr])
                if not isinstance(d, dict):
                    continue
                for (k,v) in d.items():
                    m[k] = v
                setattr(j, attr, m)
                continue
            if self.jobattrs[attr] == arc.JobState:
                # ARC svn revision 28041 or later is needed to construct
                # JobState objects with a given state. For using 3.0.3
                # set an empty job state and workaround using getArcJobInfo()
                js = arc.JobState()
                setattr(j, attr, js)
                continue
            
            setattr(j, attr, self.jobattrs[attr](str(dbinfo[attr])))
        return j
    
    def _job2db(self, job):
        '''
        Convert an arc Job object to a dictionary of column name: value
        '''
        d = {}
        for attr in self.jobattrs:
            if self.jobattrs[attr] == int or self.jobattrs[attr] == str:
                d[attr] = str(getattr(job, attr))
            elif self.jobattrs[attr] == arc.JobState:
                d[attr] = getattr(job, attr).GetGeneralState()
            elif self.jobattrs[attr] == arc.StringList:
                d[attr] = '|'.join(getattr(job, attr))
            elif self.jobattrs[attr] == arc.URL:
                d[attr] = getattr(job, attr).str().replace(r'\2f',r'/')
            elif self.jobattrs[attr] == arc.Period:
                d[attr] = str(getattr(job, attr).GetPeriod())
            elif self.jobattrs[attr] == arc.Time:
                if getattr(job, attr).GetTime() != -1:
                    d[attr] = str(getattr(job, attr).str())
            elif self.jobattrs[attr] == arc.StringStringMap:
                ssm = getattr(job, attr)
                tmpdict = dict(zip(ssm.keys(), ssm.values()))
                d[attr] = str(tmpdict)

        return d

    def _writeProxyFile(self, proxypath, proxy):
        if os.path.isfile(proxypath):
            os.remove(proxypath)
        f=open(proxypath,'w')
        f.write(proxy)
        f.close()
        
    def insertProxy(self, proxy, dn, expirytime, role='', myproxyid=''):
        '''
        Add new proxy.
          - proxy: string representation of proxy file
          - dn: DN of proxy
          - expirytime: timestamp for end of life of proxy
          - role: role of proxy
          - myproxyid: id from myproxy
        Returns id of db entrance
        '''
        c=self.getCursor()
        s="INSERT INTO proxies (proxy, dn, role, myproxyid, expirytime) VALUES ('"\
                  +proxy+"','"+dn+"','"+role+"','"+myproxyid+"','"+expirytime+"')"
        print s
        c.execute(s)
        c.execute("SELECT LAST_INSERT_ID()")
        row = c.fetchone()
        id=row['LAST_INSERT_ID()']
        proxypath=os.path.join(self.proxydir,"proxiesid"+str(id))
        c.execute("UPDATE proxies SET proxypath='"+proxypath+"' WHERE id="+str(id))
        self.conn.commit()
        self._writeProxyFile(proxypath, proxy)
        return id

    def updateProxy(self, id, desc):
        '''
        Update proxy fields specified in desc.
        '''
        s="UPDATE proxies SET "+",".join(['%s=\'%s\'' % (k, v) for k, v in desc.items()])
        s+=" WHERE id="+str(id)
        c=self.getCursor()
        c.execute(s)
        self.conn.commit()
        # rewrite proxy file if proxy was updated
        if 'proxy' in desc:
            self._writeProxyFile(self.getProxyPath(id), self.getProxy(id))

    def getProxyPath(self, id):
        '''
        Get the path to the proxy file of a proxy
        '''
        c=self.getCursor()
        c.execute("SELECT proxypath FROM proxies WHERE id="+str(id))
        row = c.fetchone()
        proxypath=row['proxypath']
        if not os.path.isfile(proxypath):
            self._writeProxyFile(proxypath)
        return proxypath

    def getProxy(self, id):
        '''
        Get the string representation of a proxy
        '''
        c=self.getCursor()
        c.execute("SELECT proxy FROM proxies WHERE id="+str(id))
        row = c.fetchone()
        proxy = row['proxy']
        return proxy
        
    def getProxiesInfo(self, select, columns=[], lock=False, expect_one=False):
        '''
        Return a list of column: value dictionaries for jobs matching select.
        If lock is True the row will be locked if possible. If expect_one is true
        only one row will be returned.
        '''
        if lock:
            select += self.addLock()
        c=self.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM proxies WHERE "+select)
        if expect_one:
            return c.fetchone()
        else:
            return c.fetchall()

    def deleteProxy(self, id):
        '''
        Delete proxy from proxies table.
        '''
        # remove file first
        proxypath=self.getProxyPath(id)
        if os.path.isfile(proxypath):
            os.remove(proxypath)
        c=self.getCursor()
        c.execute("DELETE FROM proxies WHERE id="+str(id))
        self.conn.commit()

if __name__ == '__main__':
    import random
    import logging
    import aCTConfig
    conf = aCTConfig.aCTConfigARC()
    
    adb = aCTDBArc(logging.getLogger(),dbname=conf.get(["db","file"]))
    adb.createTables()

    usercfg = arc.UserConfig("", "")
    usercfg.Timeout(10)
    
    # Simple job description which outputs hostname to stdout
    jobdescstring = "&(executable=/bin/hostname)(stdout=stdout)"
    
    # Parse job description
    jobdescs = arc.JobDescriptionList()
    if not arc.JobDescription_Parse(jobdescstring, jobdescs):
        logging.error("Invalid job description")
        exit(1)
    
    # Use top-level NorduGrid information index to find resources
    index = arc.Endpoint("ldap://index1.nordugrid.org:2135/Mds-Vo-name=nordugrid,o=grid",
                         arc.Endpoint.REGISTRY,
                         "org.nordugrid.ldapegiis")
    services = arc.EndpointList(1, index)
    
    # Do the submission
    #jobs = arc.JobList()
    #submitter = arc.Submitter(usercfg)
    #if submitter.BrokeredSubmit(services, jobdescs, jobs) != arc.SubmissionStatus.NONE:
    #    logging.error("Failed to submit job")
    #    exit(1)
     
    #adb.insertArcJob(1, jobs[0])
    #dbjob = adb.getArcJob(1)
    #print dbjob[1].JobID, dbjob[1].State.GetGeneralState()
