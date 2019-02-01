import os
import re
import signal
import subprocess
import sys
import time
import logging
from act.common import aCTLogger
from act.arc import aCTDBArc
from act.atlas import aCTDBPanda


class aCTReport:
    '''Print summary info on jobs in DB. Use --web to print html that is
    automatically refreshed'''

    def __init__(self):
        self.output = ""
        self.harvester = False
        self.outfile = None
        self.logger=aCTLogger.aCTLogger("aCTReport")
        self.actlog=self.logger()
        self.actlog.logger.setLevel(logging.ERROR)
        self.criticallogger = aCTLogger.aCTLogger('aCTCritical', arclog=False)
        self.criticallog = self.criticallogger()

        if len(sys.argv) >= 2 and sys.argv[1] == '--web':
            self.log('<META HTTP-EQUIV="refresh" CONTENT="60"><pre>')
            self.log(time.asctime() + '\n')

        self.db=aCTDBArc.aCTDBArc(self.actlog)
        self.pandadb=aCTDBPanda.aCTDBPanda(self.actlog)

    def log(self, message=''):
        self.output += message + '\n'

    def ProcessReport(self):
        actprocscmd = 'ps ax -ww -o pid,etime,args'
        p = subprocess.Popen(actprocscmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()

        if err:
            self.log('Error: could not run ps command: %s' % err)
            return

        # Group processes by cluster
        cluster_procs = {}
        longprocesses = []
        for line in out.split('\n'):
            reg = re.match(r'\s*(\d*)\s*(.*) .*python.* .*(aCT\w*)\.py\s?(\S*)', line)
            if reg:
                pid, runningtime, process, cluster = reg.groups()
                # ignore Main and this process
                if process in ['aCTReport', 'aCTMain', 'aCTHeartbeatWatchdog']:
                    continue
                if cluster == '':
                    cluster = '(no cluster defined)'
                elif not re.match(r'\d\d:\d\d$', runningtime):
                    # Check for overrunning processes
                    longprocesses.append((process, pid, cluster, runningtime))
                if cluster in cluster_procs:
                    cluster_procs[cluster].append(process)
                else:
                    cluster_procs[cluster] = [process]

        for proc in longprocesses:
            self.log('WARNING: %s (pid %s) for %s running for more than one hour (%s), this process will be killed' % proc)
            # Kill process and log a critical message to send email
            # Too many emails, disable
            #self.criticallog.critical('Killing process %s (pid %s) for %s running for more than one hour (%s)' % proc)
            try:
                os.kill(int(proc[1]), signal.SIGKILL)
            except OSError:
                pass
        self.log()
        self.log('Active processes per cluster:')
        for cluster in sorted(cluster_procs):
            procs = cluster_procs[cluster]
            procs.sort()
            self.log('%38s: %s' % (cluster, ' '.join(procs)))
        self.log()

    def PandaReport(self):
        c=self.db.db.conn.cursor()
        c.execute("select sitename, actpandastatus, corecount from pandajobs")
        rows=c.fetchall()
        rep={}
        rtot={}
        states = ["sent", "starting", "running", "slots", "tovalidate", "toresubmit",
                  "toclean", "finished", "done", "failed", "donefailed",
                  "tobekilled", "cancelled", "donecancelled"]

        self.log("All Panda jobs: %d" % len(rows))
        self.log("%29s %s" % (' ', ' '.join(['%9s' % s for s in states])))
        for r in rows:

            site, state = (str(r[0]), str(r[1]))
            if r[2] is None:
                corecount=1
            else:
                corecount=int(r[2])

            try:
                rep[site][state]+=1
                if state == "running":
                    rep[site]["slots"]+=1*corecount 
            except:
                try:
                    rep[site][state]=1
                    if state == "running":
                        try:
                            rep[site]["slots"]+=1*corecount
                        except:
                            rep[site]["slots"]=corecount
                except:
                    rep[site]={}
                    rep[site][state]=1
                    if state == "running":
                        rep[site]["slots"]=corecount 
            try:
                rtot[state]+=1
                if state == "running":
                    rtot["slots"]+=1*corecount 
            except:
                rtot[state]=1
                if state == "running":
                    rtot["slots"]=corecount 

        for k in sorted(rep.keys()):
            log="%28s:" % k[:28]
            for s in states:
                try:
                    log += '%10s' % str(rep[k][s])
                except KeyError:
                    log += '%10s' % '-'
            self.log(log)
        log = "%28s:" % "Totals"
        for s in states:
            try:
                log += '%10s' % str(rtot[s])
            except:
                log += '%10s' % '-'
        self.log(log+'\n\n')

    def ArcJobReport(self):
        c=self.db.db.conn.cursor()
        c.execute("select jobid,state from arcjobs")
        rows=c.fetchall()
        rep={}
        rtot={}
        states = ["Undefined", "Accepted", "Preparing", "Submitting",
                 "Queuing", "Running", "Finishing", "Finished", "Hold", "Killed",
                 "Failed", "Deleted", "Other"]

        self.log("All ARC jobs: %d" % len(rows))
        self.log("%39s %s" % (' ', ' '.join(['%9s' % s for s in states])))
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

        #for k in sorted(rep.keys()):
        for y in sorted([list(reversed(x.strip().split('.'))) for x in rep.keys()]):
	    k='.'.join(list(reversed(y)))
            log="%38s:" % k[:38]
            for s in states:
                try:
                    log += '%10s' % str(rep[k][s])
                except KeyError:
                    log += '%10s' % '-'
            self.log(log)
        log = "%38s:" % "Totals"
        for s in states:
            try:
                log += '%10s' % str(rtot[s])
            except:
                log += '%10s' % '-'
        self.log(log+'\n\n')

    def CondorJobReport(self):

        condorjobstatemap = ['Undefined', # used before real state is known
                             'Idle',
                             'Running',
                             'Removed',
                             'Completed',
                             'Held',
                             'Transferring',
                             'Suspended']

        c = self.db.db.conn.cursor()
        c.execute("select cluster, JobStatus from condorjobs")
        rows = c.fetchall()
        rep = {}
        rtot = {}

        self.log("All Condor jobs: %d" % len(rows))
        self.log("%29s %s" % (' ', ' '.join(['%9s' % s for s in condorjobstatemap])))
        for r in rows:

            cl = str(r[0])
            if not cl:
                cl = 'WaitingSubmission'

            jid = r[1]

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
        #for k in sorted(rep.keys()):
        for y in sorted([list(reversed(x.strip().split('.'))) for x in rep.keys()]):
	    k='.'.join(list(reversed(y)))
            log="%28s:" % k[:28]
            for s in range(8):
                try:
                    log += '%10s' % str(rep[k][s])
                except KeyError:
                    log += '%10s' % '-'
            self.log(log)
        log = "%28s:" % "Totals"
        for s in range(8):
            try:
                log += '%10s' % str(rtot[s])
            except:
                log += '%10s' % '-'
        self.log(log+'\n\n')


    def StuckReport(self):

        # Query for lost jobs older than lostlimit
        lostlimit = 86400
        select = "(arcstate='submitted' or arcstate='running') and " \
                 + self.db.timeStampLessThan("tarcstate", lostlimit) + \
                 " and sendhb=1 and arcjobs.id=pandajobs.arcjobid order by tarcstate"
        columns = ['cluster']
        jobs = self.db.getArcJobsInfo(select, columns, tables='arcjobs,pandajobs')

        if jobs:
            self.log('Found %d jobs not updated in over %d seconds:\n' % (len(jobs), lostlimit))

            clustercount = {}
            for job in jobs:
                try:
                    host = re.search('.+//([^:]+)', job['cluster']).group(1)
                except:
                    pass
                if host in clustercount:
                    clustercount[host] += 1
                else:
                    clustercount[host] = 1

            for cluster, count in clustercount.items():
                self.log('%s %s' % (count, cluster))
            self.log()
    def HarvesterReport(self):
      try:
        from distutils.sysconfig import get_python_lib # pylint: disable=import-error
        sys.path.append(get_python_lib()+'/pandacommon')

        os.environ['PANDA_HOME']=os.environ['VIRTUAL_ENV']

        from collections import defaultdict # pylint: disable=import-error
        from pandaharvester.harvestercore.db_proxy_pool import DBProxyPool as DBProxy # pylint: disable=import-error

        self.dbProxy = DBProxy()

        workers = self.dbProxy.get_worker_stats_bulk(None)
        rep = {}

        states = ["to_submit","submitted", "running"]
        rtot = defaultdict(int)

        self.log("All Harvester jobs")
        self.log( "%29s %s" % (' ', ' '.join(['%9s' % s for s in states])))
        for site, resources in workers.items():
            for resource, jobs in resources.items():
                rep['%s-%s' % (site, resource)] = jobs
                for state, count in jobs.items():
                    rtot[state] += count

        for k in sorted(rep.keys()):
            log="%28s:" % k[:28]
            for s in states:
                try:
                    log += '%10s' % str(rep[k][s])
                except KeyError:
                    log += '%10s' % '-'
            self.log(log)
        log = "%28s:" % "Totals"
        for s in states:
            try:
                log += '%10s' % str(rtot[s])
            except:
                log += '%10s' % '-'
        self.log(log+'\n\n')
      except:
        pass

    def end(self):
        if len(sys.argv) >= 2 and sys.argv[1] == '--web':
            self.log('</pre>')


def main():
    acts=aCTReport()
    args=iter(sys.argv)
    for i in args:
        if i == "--web":
            acts.outfile = args.next()
        if i == "--harvester":
            acts.harvester = True
    acts.PandaReport()
    if acts.harvester:
        acts.HarvesterReport()
    acts.ArcJobReport()
    acts.CondorJobReport()
    acts.StuckReport()
    acts.ProcessReport()
    acts.end()
    if acts.outfile is None:
        sys.stdout.write(acts.output)
    else:
        f=open(acts.outfile,"w")
        f.write(acts.output)
        f.close()

if __name__ == '__main__':
    main()
