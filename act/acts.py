#!/usr/bin/python

import aCTDB
import aCTConfig
import re
import sys

filter=""

class aCTStatus:
    
    def __init__(self):
        self.conf=aCTConfig.aCTConfig()
        self.db=aCTDB.aCTDB(None,self.conf.get(["db","file"]))


    def JobReport(self):
        c=self.db.conn.cursor()
	print filter
        c.execute("select arcjobid,arcstatus from jobs "+ filter)
        rows=c.fetchall()
        rep={}
        rtot={}

        print "All jobs: %d" % len(rows)
        for r in rows:

            reg=re.search('.+//([^:]+)',str(r['arcjobid']))
            cl=""
            try:
                cl=reg.group(1)
            except:
                cl='WaitingSubmission'

            jid=str(r['arcstatus'])
            if jid is None:
                jid="Waiting state"

            try:
                rep[cl][jid]+=1
            except:
                try:
                    rep[cl][jid]=1
                except:
                    rep[cl]={}
                    rep[cl][jid]=1
            try:
                rtot[jid]+=1
            except:
                rtot[jid]=1

        for k in rep.keys():
            log="%28s:" % k
            for s in rep[k].keys():
                log += " "+s+": %d" % rep[k][s]
            print log
        print "Totals:"
        for k in rtot.keys():
            print "  %s: %d" % (k,rtot[k])

        
try:
  filter=sys.argv[1]
except:
  pass
acts=aCTStatus()
acts.JobReport()
