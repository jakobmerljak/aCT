#!/usr/bin/python

import aCTConfig
import re
import subprocess
import aCTLogger
from act.arc import aCTDBArc

class aCTStatus:
    
    def __init__(self):
        self.conf=aCTConfig.aCTConfigARC()
        self.logger=aCTLogger.aCTLogger("panda2arc")
        self.log=self.logger()


        #self.db=aCTDB.aCTDB(None,self.conf.get(["db","file"]))
        self.db=aCTDBArc.aCTDBArc(self.log,self.conf.get(["db","file"]))

    def ProcessReport(self):
        actprocscmd = 'ps ax -ww -o etime,args'
        p = subprocess.Popen(actprocscmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        
        if err:
            print 'Error: could not run ps command: %s' % err
            return
        
        # Group processes by cluster
        cluster_procs = {}
        longprocesses = []
        for line in out.split('\n'):
            reg = re.match('\s*(.*) /.*python.* .*(aCT\w*)\.py\s?(\S*)', line)
            if reg:
                runningtime, process, cluster = reg.groups()
                # ignore Main and this process
                if process == 'aCTReport' or process == 'aCTMain':
                    continue
                if cluster == '':
                    cluster = '(no cluster defined)'
                elif not re.match('\d\d:\d\d', runningtime):
                    # Check for overrunning processes
                    longprocesses.append((process, cluster))
                if cluster in cluster_procs:
                    cluster_procs[cluster].append(process)
                else:
                    cluster_procs[cluster] = [process]
        
        print 'Active processes per cluster:'
        for cluster, procs in cluster_procs.items():
            procs.sort()
            print '%28s: %s' % (cluster, ' '.join(procs))
        print
        for proc in longprocesses:
            print 'WARNING: %s for %s running for more than one hour' % proc
        print

    def JobReport(self):
        c=self.db.conn.cursor()
        c.execute("select jobid,state from arcjobs")
        rows=c.fetchall()
        rep={}
        rtot={}
        states = ["Undefined", "Accepted", "Preparing", "Submitting",
                 "Queuing", "Running", "Finishing", "Finished", "Hold", "Killed",
                 "Failed", "Deleted", "Other"]

        print "All jobs: %d" % len(rows)
        print "%29s %s" % (' ', ' '.join(['%9s' % s for s in states]))
        for r in rows:

            reg=re.search('.+//([^:]+)',str(r[0]))
            cl=""
            try:
                cl=reg.group(1)
            except:
                cl='WaitingSubmission'

            jid=str(r[1])
            if jid == 'None':
                jid="Other"

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
            log="%28s:" % k[:28]
            for s in states:
                try:
                    log += '%10s' % str(rep[k][s])
                except KeyError:
                    log += '%10s' % '-'
            print log
        log = "%28s:" % "Totals"
        for s in states:
            try:
                log += '%10s' % str(rtot[s])
            except:
                log += '%10s' % '-'
        print log

        

acts=aCTStatus()
acts.ProcessReport()
acts.JobReport()

