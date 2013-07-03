import os
import MySQLdb as DB
import time
import datetime
import aCTJob
import arc

class aCTDB:

    def __init__(self,logger,socket="/tmp/act.mysql.socket",dbname="act"):
        self.conn=DB.connect(unix_socket=socket,db=dbname)

    def dropTables(self):

        c=self.conn.cursor()
        c.execute("drop table if exists arcjobs")
        c.execute("drop table if exists pandajobs")

    def createDatabase(self):
        c=self.conn.cursor()
        c.execute("create database act")
        
    def createTables(self):

        self.dropTables()

        # arcjobs
        str="""
create table arcjobs (
  id integer primary key auto_increment,
  modified timestamp,
  created timestamp,

  JobID varchar(256),
  Name  varchar(256),
  ServiceInformationURL varchar(256),
  ServiceInformationInterfaceName varchar(256),
  JobStatusURL varchar(256),
  JobStatusInterfaceName varchar(256),
  JobManagementURL varchar(256),
  JobManagementInterfaceName varchar(256),

  StageInDir varchar(256),
  StageOutDir varchar(256),
  SessionDir varchar(256),

  Type varchar(256),
  IDFromEndpoint varchar(256),
  LocalIDFromManager varchar(256),
  JobDescription text,
  JobDescriptionDocument text,

  State varchar(256),
  RestartState varchar(256),
  ExitCode integer,
  ComputingManagerExitCode varchar(256),
  Error text,
  WaitingPosition integer,
  UserDomain varchar(256),
  Owner varchar(256),
  LocalOwner varchar(256),
  RequestedTotalWallTime integer,
  RequestedTotalCPUTime integer,
  RequestedSlots integer,
  RequestedApplicationEnvironment text,
  StdIn varchar(256),
  StdOut varchar(256),
  StdErr varchar(256),
  LogDir varchar(256),
  ExecutionNode text,
  Queue varchar(256),
  UsedTotalWallTime integer,
  UsedTotalCPUTime integer,
  UsedMainMemory integer,
  LocalSubmissionTime datetime,
  SubmissionTime datetime,
  ComputingManagerSubmissionTime datetime,
  StartTime datetime,
  ComputingManagerEndTime datetime,
  EndTime datetime,
  WorkingAreaEraseTime datetime,
  ProxyExpirationTime datetime,
  SubmissionHost varchar(256),
  SubmissionClientName varchar(256),
  CreationTime datetime,
  Validity integer,
  OtherMessages text,
  ActivityOldID text,
  LocalInputFiles text,

  externalid int,
  tstate timestamp,
  action varchar(80),
  description text,
  ndownloadretries int default 0,
  nuploadretries int default 0,
  ioload int default 0,
  site varchar(80),
  clusters varchar(256) default 'any',
  proxyfile varchar(128),
  priority int default 0,
  cluster varchar(256) default '',
  pid int default 0
  
  
)
"""

        str1="""
create table arcjobs (
  id integer primary key auto_increment,
  modified timestamp,
  created timestamp,
"""
        aj=aCTJob.aCTJob()
        str1+=aj.DbColumnsCreate()
        str1+="""
  externalid int,
  tstate timestamp,
  action varchar(80),
  description text,
  ndownloadretries int default 0,
  nuploadretries int default 0,
  ioload int default 0,
  site varchar(80),
  clusters varchar(256) default 'any',
  proxyfile varchar(128),
  priority int default 0,
  cluster varchar(256) default '',
  pid int default 0
)
"""
        print str1
        c=self.conn.cursor()
        c.execute(str1)
        c.execute("create index ijobid on arcjobs (jobid)")
        c.execute("create index iaction on arcjobs (action)")
        c.execute("create index iclusters on arcjobs (clusters)")

        str="""
create table pandajobs (
  id integer primary key auto_increment,
  pandaid integer unique,
  modified timestamp,
  created timestamp,
  status varchar(80),
  job text not null, 
  sitename varchar(80),
  prodsourcelabel varchar(80),
  action varchar(80),
  nresubmits int default 0,
  theartbeat timestamp
)
"""
        c.execute(str)

    def Execute(self,sql):
        c = self.conn.cursor(DB.cursors.DictCursor)
        try:
            c.execute(sql)
            p=c.fetchall()
            self.conn.commit()
        except:
            return None
        return p


    def ExecuteNoCommit(self,sql):
        c = self.conn.cursor(DB.cursors.DictCursor)
        try:
            c.execute(sql)
            p=c.fetchall()
        except:
            return None
        return p
    
    def Commit(self):
        self.conn.commit()

    def insertArcJob(self,desc):
        s="insert into arcjobs ( "+",".join(['%s' % (k) for k in desc.keys()]) + ") values "
        s+=" ( " + ",".join(['%s'] * len(desc))  +" )"
        #print s
        c=self.conn.cursor()
        c.execute(s,(desc.values()))
        self.conn.commit()

    def updateArcJob(self,id,desc):
        desc['modified']=datetime.datetime.fromtimestamp(time.time())
        s="update arcjobs set "+",".join(['%s=%%s' % (k) for k in desc.keys()])
        s+=" where id="+str(id)
        #print s
        c=self.conn.cursor()
        c.execute(s,(desc.values()))
        self.conn.commit()

    def getArcJobs(self,where):
        #print where
        #print "select * from arcjobs where %s" % where
        return self.Execute("select * from arcjobs where %s" % where)
        
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

if __name__ == '__main__':

    db=aCTDB("")

    # create tables
    db.dropTables()
    db.createTables()

    for i in range(1,21):
        ts=datetime.datetime.fromtimestamp(time.time())
        c=db.conn.cursor()
        c.execute("insert into arcjobs (created,cluster) values (%s, %s)",(ts,"pikolit.ijs.si"))
        #c.execute("insert into arcjobs (id,created) values ("+str(i)+") ")

    p=db.Execute("select * from arcjobs")
    for i in p:
        print i
        print i['id'],i['modified']
    p=db.Execute("select count(*) from arcjobs")
    print p[0]['count(*)']

    #time.sleep(2)
    desc={}
    desc['error']="test error"
    db.updateArcJob(1,desc)


    p=db.Execute("select * from arcjobs where id=1")
    print p[0]
    
    p=db.Execute("delete from arcjobs")

    # import pysqlite2.dbapi2 as sqlite
    # connl=sqlite.connect("aCTjobs.sqlite",1800)
    # connl.row_factory=dict_factory
    # cl=connl.cursor()
    # p=cl.execute("select * from arcjobs")
    # f=open("/tmp/x509up_u3202")
    # proxy="".join(f.readlines())
    # f.close()
    # for r in p:
    #     i={}
    #     i['tstate']=datetime.datetime.fromtimestamp((r['tstate']))
    #     i['error']=r['errors']
    #     i['exitcode']=r['exitcode']
    #     i['cluster']=r['cluster']
    #     i['proxy']=str(proxy)
    #     db.insertArcJob(i)

    # p=db.Execute("select proxy from arcjobs where id=300")
    # print p[0]['proxy']

    # f=open("0000","w")
    # f.write(p[0]['proxy'])
    # f.close()

    j=arc.Job()
    j.Name='test'
    jj=aCTJob.aCTJob()

    for s in jj.attrs[str]:
        print s,getattr(j,s)
