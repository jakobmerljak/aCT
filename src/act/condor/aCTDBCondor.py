from act.db.aCTDB import aCTDB
from act.common import aCTConfig

class aCTDBCondor(aCTDB):

    def __init__(self,logger,dbname="aCTjobs.db"):
        aCTDB.__init__(self, logger, dbname)


    def createTables(self):
        '''
        condorjobs: ClassAds have too many fields to make a column per field so
        only the most relevant fields are stored
          - id: unique (internal) id of job
          - created: timestamp of creation of the record
          - modified: timestamp of last record update
          - condorstate: tosubmit, submitting, submitted, running, stalled, tocancel,
                      cancelling, cancelled, finished, failed, tofetch, torerun,
                      toresubmit, done, donefailed, lost, toclean
            "to" states are set by application engine or Condor engine for retries
          - tcondorstate: time stamp of last arcstate
          - tstate: time stamp of last arc Job state change
          - cluster: hostname of the cluster chosen for the job
          - clusterlist: comma separated list of clusters on which the job may
            run. Can be empty.
          - jobdesc: Row id in jobdescriptions table
          - attemptsleft: Number of attempts left to run the job
          - downloadfiles: Semicolon-separated list of specific files to download
            after job finished. If empty download all in job desc.
          - proxyid: id of corresponding proxies entry of proxy to use for this job
          - appjobid: job identifier of application. Used in log messages to track
            a job through the system
          - priority: ARC job priority, extracted from the job description
          - fairshare: A string representing a share. Job submission for the same
            cluster will be spread evenly over shares.
        ClassAd fields:
          - ClusterID
          - GlobalJodId
          - JobStatus
          - RemoteWallClockTime
          - RemoteUserCpu
          - ExitCode
        '''

        # in MySQL the first timestamp specified gets automatically updated to
        # current time for each change. 
        create="""
            CREATE TABLE condorjobs (
            id INTEGER PRIMARY KEY AUTO_INCREMENT,
            modified TIMESTAMP,
            created TIMESTAMP,
            condorstate VARCHAR(255),
            tcondorstate TIMESTAMP,
            tstate TIMESTAMP,
            cluster VARCHAR(255),
            clusterlist VARCHAR(1024),
            jobdesc INT(11),
            attemptsleft INTEGER,
            downloadfiles VARCHAR(255),
            proxyid INTEGER,
            appjobid VARCHAR(255),
            priority SMALLINT,
            fairshare VARCHAR(255),
            ClusterId BIGINT,
            GlobalJobId VARCHAR(255),
            GridJobId VARCHAR(255),
            JobStatus SMALLINT DEFAULT 0 NOT NULL,
            RemoteWallClockTime FLOAT,
            RemoteUserCpu FLOAT,
            ExitCode SMALLINT
            )
            """
            
        # First check if table already exists
        c = self.getCursor()
        c.execute("show tables like 'condorjobs'")
        row = c.fetchone()
        self.conn.commit()
        if row:
            answer = raw_input("Table condorjobs already exists!\nAre you sure you want to recreate it? (y/n) ")
            if answer != 'y':
                return
            c.execute("drop table condorjobs")

        # Create condorjobs
        try:
            c.execute(create)
            self.conn.commit()
        except Exception,x:
            self.log.error("failed create table %s" %x)


    def Commit(self, lock=False):
        if lock:
            res = self.releaseMutexLock('condorjobs')
            if not res:
                self.log.warning("Could not release lock: %s" % str(res))
        self.conn.commit()


    def insertCondorJobDescription(self, jobdesc, proxyid='', maxattempts=0, clusterlist='', appjobid='', fairshare=''):
        '''
        Add a new job description (ClassAd object) for the Condor engine to
        process.
        '''
        
        if not clusterlist:
            self.log.error('%s: clusterlist cannot be empty for condor jobs' % appjobid)
            return None

        # todo: find some useful default for proxyid
        c = self.getCursor()
        
        s = "insert into jobdescriptions (jobdescription) values (%s)"
        c.execute(s, [str(jobdesc)])
        c.execute("SELECT LAST_INSERT_ID()")
        jobdescid = c.fetchone()['LAST_INSERT_ID()']
        
        desc = {}
        desc['created'] = self.getTimeStamp()
        desc['condorstate'] = "tosubmit"
        desc['tcondorstate']  = desc['created']
        desc['tstate'] = desc['created']
        desc['cluster']  = ''
        desc['clusterlist'] = clusterlist
        desc['jobdesc'] = jobdescid
        desc['attemptsleft'] = maxattempts
        desc['proxyid'] = proxyid
        desc['appjobid'] = appjobid
        desc['priority'] = jobdesc['JobPrio']
        desc['fairshare'] = fairshare
        s="insert into condorjobs" + " ( " + ",".join(['%s' % (k) for k in desc.keys()]) + " ) " + " values " + \
            " ( " + ",".join(['%s' % (k) for k in ["%s"] * len(desc.keys()) ]) + " ) "
        c.execute(s,desc.values())
        c.execute("SELECT LAST_INSERT_ID()")
        row = c.fetchone()
        self.conn.commit()
        return row
        

    def deleteCondorJob(self, id):
        '''
        Delete job from Condor table.
        '''
        c = self.getCursor()
        c.execute("select jobdesc from condorjobs where id="+str(id))
        row = c.fetchone()
        if row:
            c.execute("delete from jobdescriptions where id="+str(row['jobdesc']))
        c.execute("delete from condorjobs where id="+str(id))
        self.conn.commit()

    def updateCondorJob(self, id, desc):
        '''
        Update condor job fields specified in desc
        '''
        self.updateCondorJobLazy(id, desc)
        self.conn.commit()

    def updateCondorJobLazy(self, id, desc):
        '''
        Update condor job fields specified in desc. Does not commit after
        executing update.
        '''
        desc['modified'] = self.getTimeStamp()
        s = "update condorjobs set " + ",".join(['%s=%%s' % (k) for k in desc.keys()])
        s += " where id="+str(id)
        c = self.getCursor()
        c.execute("select id from condorjobs where id="+str(id))
        row = c.fetchone()
        if row is None:
            self.log.warning("Condor job id %d no longer exists" % id)
            return
        c.execute(s, desc.values())

    def updateCondorJobs(self, desc, select):
        '''
        Update condor job fields specified in desc and matching the select
        statement.
        '''
        self.updateCondorJobsLazy(desc, select)
        self.conn.commit()

    def updateCondorJobsLazy(self, desc, select):
        '''
        Update condor job fields specified in desc and matching the select
        statement. Does not commit after executing update.
        '''
        desc['modified'] = self.getTimeStamp()
        s = "update condorjobs set " + ",".join(['%s=%%s' % (k) for k in desc.keys()])
        s += " where "+select
        c = self.getCursor()
        c.execute(s, desc.values())

    def getCondorJobInfo(self, id, columns=[]):
        '''
        Return a dictionary of column name: value for the given id and columns
        ''' 
        c = self.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM condorjobs WHERE id="+str(id))
        row = c.fetchone()
        if not row:
            return {}
        # mysql SELECT returns list, we want dict
        if not isinstance(row, dict):
            row = dict(zip([col[0] for col in c.description], row))
        return row

    def getCondorJobsInfo(self, select, columns=[], tables="condorjobs", lock=False):
        '''
        Return a list of column: value dictionaries for jobs matching select.
        If lock is True the row will be locked if possible.
        '''
        c=self.getCursor()
        if lock:
            #select += self.addLock()
            res = self.getMutexLock('arcjobs', timeout=2)
            if not res:
                self.log.debug("Could not get lock: %s"%str(res))
                return []
        c.execute("SELECT "+self._column_list2str(columns)+" FROM "+tables+" WHERE "+select)
        rows=c.fetchall()
        return rows

    def getCondorJobDescription(self, jobdescid):
        '''
        Return the job description for the given id in jobdescriptions
        '''
        c = self.getCursor()
        c.execute("SELECT jobdescription from jobdescriptions where id="+str(jobdescid))
        row = c.fetchone()
        if not row:
            return None
        return row['jobdescription']
    
    def getActiveClusters(self):
        '''
        Return a list and count of clusters
        '''
        c = self.getCursor()
        c.execute("SELECT cluster, COUNT(*) FROM condorjobs WHERE cluster!='' GROUP BY cluster")
        rows = c.fetchall()
        return rows

    def getClusterLists(self):
        '''
        Return a list and count of clusterlists for jobs to submit
        '''
        c = self.getCursor()
        # submitting state is included here so that a submitter process is not
        # killed while submitting jobs
        c.execute("SELECT clusterlist, COUNT(*) FROM condorjobs WHERE condorstate='tosubmit' OR condorstate='submitting' GROUP BY clusterlist")
        rows = c.fetchall()
        return rows

if __name__ == '__main__':
    import logging, sys
    log = logging.getLogger()
    out = logging.StreamHandler(sys.stdout)
    log.addHandler(out)

    conf = aCTConfig.aCTConfigARC()

    adb = aCTDBCondor(log, dbname=conf.get(["db","file"]))
    adb.createTables()
