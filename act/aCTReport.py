#!/usr/bin/python

import aCTDBArc
import aCTConfig
import re
import subprocess
import aCTLogger

class aCTStatus:
    
    def __init__(self):
        self.conf=aCTConfig.aCTConfigARC()
        self.logger=aCTLogger.aCTLogger("panda2arc")
        self.log=self.logger()


        #self.db=aCTDB.aCTDB(None,self.conf.get(["db","file"]))
        self.db=aCTDBArc.aCTDBArc(self.log,self.conf.get(["db","file"]))

    def ProcessReport(self):
        actprocscmd = 'ps ax -ww -o args'
        p = subprocess.Popen(actprocscmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        
        if err:
            print 'Error: could not run ps command: %s' % err
            return
        
        # Group processes by cluster
        cluster_procs = {}
        for line in out.split('\n'):
            reg = re.match('.*python.* .*(aCT\w*)\.py\s?(\S*)', line)
            if reg:
                process, cluster = reg.groups()
                # ignore Main and this process
                if process == 'aCTReport' or process == 'aCTMain':
                    continue
                if cluster == '':
                    cluster = '(no cluster defined)'
                if cluster in cluster_procs:
                    cluster_procs[cluster].append(process)
                else:
                    cluster_procs[cluster] = [process]
        
        print 'Active processes per cluster:'
        for cluster, procs in cluster_procs.items():
            procs.sort()
            print '%28s: %s' % (cluster, ' '.join(procs))
        print

    def JobReport(self):
        c=self.db.conn.cursor()
        c.execute("select jobid,state from arcjobs")
        rows=c.fetchall()
        rep={}
        rtot={}

        print "All jobs: %d" % len(rows)
        for r in rows:

            reg=re.search('.+//([^:]+)',str(r[0]))
            cl=""
            try:
                cl=reg.group(1)
            except:
                cl='WaitingSubmission'

            jid=str(r[1])
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
acts.ProcessReport()
acts.JobReport()
