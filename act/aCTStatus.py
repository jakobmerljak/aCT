#!/usr/bin/python

import aCTDBPanda
import aCTConfig
import re

class aCTStatus:
    
    def __init__(self):
        self.conf=aCTConfig.aCTConfig()
        self.db=aCTDBPanda.aCTDBPanda(None,self.conf.get(["db","file"]))


    def JobReport(self):
        rows=self.db.getJobReport()
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

        

acts=aCTStatus()
acts.JobReport()
