import os
import pysqlite2.dbapi2 as sqlite
import time
from aCTDB import aCTDB

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class aCTDBArc(aCTDB):

    def createTables(self):
        # arcjobs:
        #   - pandaid:
        #   - tstamp: timestamp of last record update
        #   - tarcstatus: time stamp of last arcstatus ?
        #   - cluster: hostname of the cluster
        #   - submissiontime: time stamp of submission
        #   - completiontime: time stamp of FINISHED, FAILED
        #   - usedcputime:
        #   - usedwalltime:
        #   - errors: as reported by arc
        #   - exitcode:
        #   - executionnodes:
        #   - proxyexpiretime: time stamp of proxy
        #   - rerunable:
        aCTDB.createTables(self)
        str="create table arcjobs (pandaid integer, tstamp timestamp, tarcstatus timestamp, cluster text,  submissiontime timestamp, completiontime timestamp, usedcputime integer, usedwalltime integer, errors text, exitcode integer, executionnodes text, proxyexpiretime timestamp, rerunable text, xml text, log text, starttime timestamp, endtime timestamp)"
        c=self.conn.cursor()
        try:
            c.execute("drop table arcjobs")
        except:
            self.log.warning("no arcjobs table")
            pass
        try:
            c.execute(str)
            self.conn.commit()
        except Exception,x:
            self.log.error("failed create table %s" %x)
            pass

    def insertArcJob(self,pandaid):
        c=self.conn.cursor()
        c.execute("insert into arcjobs (tstamp,pandaid) values ("+str(time.time())+","+str(pandaid)+")")
        self.conn.commit()

    def deleteArcJob(self,pandaid):
        c=self.conn.cursor()
        c.execute("delete from arcjobs where pandaid="+str(pandaid))
        self.conn.commit()

    def updateArcJob(self,id,desc):
        desc['tstamp']=time.time()
        s="update arcjobs set "+",".join(['%s=\'%s\'' % (k, v) for k, v in desc.items()])
        s+=" where pandaid="+str(id)
        c=self.conn.cursor()
        c.execute("select pandaid from arcjobs where pandaid="+str(id))
        row=c.fetchone()
        if(row is None) :
            self.insertArcJob(id)
        c.execute(s)
        self.conn.commit()

    def updateArcJobLazy(self,id,desc):
        desc['tstamp']=time.time()
        s="update arcjobs set "+",".join(['%s=\'%s\'' % (k, v) for k, v in desc.items()])
        s+=" where pandaid="+str(id)
        c=self.conn.cursor()
        c.execute("select pandaid from arcjobs where pandaid="+str(id))
        row=c.fetchone()
        if(row is None) :
            self.insertArcJob(id)
        c.execute(s)

    def getArcJob(self,pandaid,columns=[]):
        c=self.conn.cursor()
        print "SELECT "+self._column_list2str(columns)+" FROM arcjobs WHERE pandaid="+str(pandaid)
        c.execute("SELECT "+self._column_list2str(columns)+" FROM arcjobs WHERE pandaid="+str(pandaid))
        row=c.fetchone()
        # mysql SELECT returns list, we want dict
        # todo: cx_oracle probably has no column_names list. need to figure out a more general approach
        if not isinstance(row,dict):
            row=dict(zip(c.column_names,row))
        return row

    def getArcJobs(self,select, columns=[]):
        c=self.conn.cursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM arcjobs WHERE "+select)
        rows=c.fetchall()
        # mysql SELECT returns list, we want dict
        # todo: cx_oracle probably has no column_names list. need to figure out a more general approach
        if not isinstance(rows,dict):
            rows=dict(zip(c.column_names, zip(*[list(row) for row in rows])))
        return rows

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    #adb=aCTDB(logging.getLogger('test'),dbname='test.sqlite')
    adb=aCTDBArc(logging.getLogger('test'))
    adb.createTables()
    adb.insertArcJob(0)
    adb.insertArcJob(1)
    aj=adb.getArcJobs("errors is NULL",columns=['executionnodes','exitcode','errors','usedwalltime','usedcputime','starttime','endtime'])
    #aj=adb.getArcJobs("errors is NULL")
    print aj['errors']
    exit(0)
