import re
import time
import arc
from aCTDB import aCTDB

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class aCTDBArc(aCTDB):
    
    def __init__(self,logger,dbname="aCTjobs.db"):
        aCTDB.__init__(self, logger, dbname)
        
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
          - pandaid:
          - tstamp: timestamp of last record update
          - arcstatus: tosubmit, submitted, running, tocancel, cancelled, finished, failed
            "to" states are set by application engine
          - tarcstatus: time stamp of last arcstatus
          - cluster: hostname of the cluster
          - jobdesc: job description added by the application engine
          - rerunnable:
        '''
        aCTDB.createTables(self)
        create="create table arcjobs ("+",".join(['%s %s' % (k, self.jobattrmap[v]) for k, v in self.jobattrs.items()])+ \
            ", pandaid integer, tstamp timestamp, tarcstatus timestamp, cluster text, jobdesc text, rerunable text)"
        c=self.conn.cursor()
        try:
            c.execute("drop table arcjobs")
        except:
            self.log.warning("no arcjobs table")
        try:
            c.execute(create)
            self.conn.commit()
        except Exception,x:
            self.log.error("failed create table %s" %x)

    def insertArcJob(self, pandaid, job):
        '''
        Add new arc Job object. Only used for testing and recreating db.
        '''
        c=self.conn.cursor()
        j = self._job2db(job)
        c.execute("insert into arcjobs (tstamp,pandaid,"+",".join(j.keys())+") values ("+str(time.time())+","+str(pandaid)+",'"+"','".join(j.values())+"')")
        self.conn.commit()
        
    def insertArcJobDescription(self, pandaid, jobdesc, cluster=''):
        '''
        Add a new job description for the ARC engine to process. If specified
        the job will be sent to the given cluster.
        '''
        c=self.conn.cursor()
        c.execute("insert into arcjobs (tstamp,pandaid,arcstatus,tarcstatus,cluster,jobdesc) values ("
                  +str(time.time())+","+str(pandaid)+",tosubmit,"+str(time.time())+","+cluster+","+jobdesc+")")
        self.conn.commit()
        

    def deleteArcJob(self, pandaid):
        '''
        Delete job from ARC table.
        '''
        c=self.conn.cursor()
        c.execute("delete from arcjobs where pandaid="+str(pandaid))
        self.conn.commit()

    def updateArcJob(self, pandaid, desc, job=None):
        '''
        Update arc job fields specified in desc and fields represented by arc
        Job if job is specified.
        '''
        self.updateArcJobLazy(pandaid, desc, job)
        self.conn.commit()

    def updateArcJobLazy(self, pandaid, desc, job=None):
        '''
        Update arc job fields specified in desc and fields represented by arc
        Job if job is specified. Does not commit after executing update.
        '''
        desc['tstamp']=time.time()
        s="update arcjobs set "+",".join(['%s=\'%s\'' % (k, v) for k, v in desc.items()])
        if job:
            s+=","+",".join(['%s=\'%s\'' % (k, v) for k, v in self._job2db(job).items()])
        s+=" where pandaid="+str(id)
        c=self.conn.cursor()
        c.execute("select pandaid from arcjobs where pandaid="+str(id))
        row=c.fetchone()
        if row is None:
            self.insertArcJob(id)
        c.execute(s)

    def getArcJobInfo(self,pandaid,columns=[]):
        '''
        Return a dictionary of column name: value for the given id and columns
        ''' 
        c=self.conn.cursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM arcjobs WHERE pandaid="+str(pandaid))
        row=c.fetchone()
        # mysql SELECT returns list, we want dict
        if not isinstance(row,dict):
            row=dict(zip([col[0] for col in c.description], row))
        return row

    def getArcJob(self, pandaid):
        '''
        Return a dictionary of pandaid: arc.Job.
        '''
        c=self.conn.cursor()
        c.execute("SELECT "+",".join(self.jobattrs.keys())+" FROM arcjobs WHERE pandaid="+str(pandaid))
        row = c.fetchone()
        if not isinstance(row,dict):
            row=dict(zip([col[0] for col in c.description], row))
        return {pandaid: self._db2job(row)}
        
        
    def getArcJobs(self,select):
        '''
        Return a dictionary of pandaid: arc.Job for jobs matching select
        '''
        c=self.conn.cursor()
        c.execute("SELECT pandaid,"+",".join(self.jobattrs.keys())+" FROM arcjobs WHERE "+select)
        rows=c.fetchall()
        # mysql SELECT returns list, we want dict
        if not isinstance(rows,dict):
            rows=dict(zip([col[0] for col in c.description], zip(*[list(row) for row in rows])))
        d = {}
        for row in rows:
            d[row[0]] = self._db2job(dict(zip([col[0] for col in c.description], row[1:])))
        return d
    
    def _db2job(self, dbinfo):
        '''
        Convert a dictionary of DB key value into arc Job object
        '''
        j = arc.Job()
        for attr in self.jobattrs:
            if attr not in dbinfo:
                continue
            # JobState cannot be constructed using public API, so ignore...
            if self.jobattrs[attr] == arc.JobState:
                continue
            # StringStringMap needs special treatment
            if self.jobattrs[attr] == arc.StringStringMap:
                m = arc.StringStringMap()
                d = eval(dbinfo[attr])
                if not isinstance(d, dict):
                    continue
                for (k,v) in d.items():
                    m[k] = v
                setattr(j, attr, m)
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
                d[attr] = getattr(job, attr).str()
            elif self.jobattrs[attr] == arc.Period:
                d[attr] = str(getattr(job, attr).GetPeriod())
            elif self.jobattrs[attr] == arc.Time:
                d[attr] = str(getattr(job, attr).GetTime())
            elif self.jobattrs[attr] == arc.StringStringMap:
                ssm = getattr(job, attr)
                tmpdict = dict(zip(ssm.keys(), ssm.values()))
                d[attr] = str(tmpdict)

        return d

if __name__ == '__main__':
    import logging
    adb = aCTDBArc(logging.getLogger('test'))
    adb.createTables()
    
    usercfg = arc.UserConfig("", "")
    
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
    jobs = arc.JobList()
    submitter = arc.Submitter(usercfg)
    if submitter.BrokeredSubmit(services, jobdescs, jobs) != arc.SubmissionStatus.NONE:
        logging.error("Failed to submit job")
        exit(1)
        
    adb.insertArcJob(1, jobs[0])
    dbjob = adb.getArcJob(1)
    print dbjob[1].JobID
