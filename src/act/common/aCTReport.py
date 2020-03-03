import argparse
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
    automatically refreshed. Add filenames to query more than one aCT DB'''

    def __init__(self, args):
        self.output = ""
        self.harvester = args.harvester
        self.outfile = args.web
        self.actconfs = args.conffiles or [''] # empty string for default behaviour

        self.logger=aCTLogger.aCTLogger("aCTReport")
        self.actlog=self.logger()
        self.actlog.logger.setLevel(logging.ERROR)
        self.criticallogger = aCTLogger.aCTLogger('aCTCritical', arclog=False)
        self.criticallog = self.criticallogger()

        if self.outfile:
            self.log('<META HTTP-EQUIV="refresh" CONTENT="60"><pre>')
            self.log(time.asctime() + '\n')

        self.db=aCTDBArc.aCTDBArc(self.actlog)
        self.pandadb=aCTDBPanda.aCTDBPanda(self.actlog)

    def log(self, message=''):
        self.output += message + '\n'

    def ProcessReport(self):
        if self.actconfs != ['']:
            return # don't print processes for combined report
        actprocscmd = 'ps ax -ww -o pid,etime,args'
        try:
            out = subprocess.run(actprocscmd.split(), check=True, encoding='utf-8', stdout=subprocess.PIPE).stdout
        except subprocess.CalledProcessError as e:
            self.log('Error: could not run ps command: %s' % e.stderr)
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
            self.log(f'{cluster:>38.38}: {" ".join(procs)}')
        self.log()

    def PandaReport(self):
        rep={}
        rtot={}
        states = ["sent", "starting", "running", "slots", "tovalidate", "toresubmit",
                  "toclean", "finished", "done", "failed", "donefailed",
                  "tobekilled", "cancelled", "donecancelled"]

        for conf in self.actconfs:
            if conf:
                os.environ['ACTCONFIGARC'] = conf

            db=aCTDBArc.aCTDBArc(self.actlog)
            c=db.db.conn.cursor()
            c.execute("select sitename, actpandastatus, corecount from pandajobs")
            rows=c.fetchall()
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

        self.log(f"All Panda jobs: {sum([v for k,v in rtot.items() if k != 'slots'])}")
        self.log(f"{'':29} {' '.join([f'{s:>9}' for s in states])}")

        for k in sorted(rep.keys()):
            log=f"{k:>28.28}:"
            for s in states:
                try:
                    log += f'{rep[k][s]:>10}'
                except KeyError:
                    log += f'{"-":>10}'
            self.log(log)
        log = f'{"Totals":>28}:'
        for s in states:
            try:
                log += f'{rtot[s]:>10}'
            except:
                log += f'{"-":>10}'
        self.log(log+'\n\n')

    def ArcJobReport(self):
        rep={}
        rtot={}
        states = ["Undefined", "Accepted", "Preparing", "Submitting",
                 "Queuing", "Running", "Finishing", "Finished", "Hold", "Killed",
                 "Failed", "Deleted", "Other"]

        for conf in self.actconfs:
            if conf:
                os.environ['ACTCONFIGARC'] = conf

            db=aCTDBArc.aCTDBArc(self.actlog)
            c=db.db.conn.cursor()
            c.execute("select jobid,state from arcjobs")
            rows=c.fetchall()
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

        self.log(f"All ARC jobs: {sum(rtot.values())}")
        self.log(f"{'':39} {' '.join([f'{s:>9}' for s in states])}")

        for k in sorted(rep, key=lambda x: x.split('.')[-1]):
            log=f"{k:>38.38}:"
            for s in states:
                try:
                    log += f'{rep[k][s]:>10}'
                except KeyError:
                    log += f'{"-":>10}'
            self.log(log)
        log = f"{'Totals':>38}:"
        for s in states:
            try:
                log += f'{rtot[s]:>10}'
            except:
                log += f'{"-":>10}'
        self.log(log+'\n\n')

    def CondorJobReport(self):

        rep = {}
        rtot = {}
        condorjobstatemap = ['Undefined', # used before real state is known
                             'Idle',
                             'Running',
                             'Removed',
                             'Completed',
                             'Held',
                             'Transferring',
                             'Suspended']

        for conf in self.actconfs:
            if conf:
                os.environ['ACTCONFIGARC'] = conf

            db=aCTDBArc.aCTDBArc(self.actlog)
            c = db.db.conn.cursor()
            c.execute("select cluster, JobStatus from condorjobs")
            rows = c.fetchall()

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

        self.log(f"All Condor jobs: {sum(rtot.values())}")
        self.log(f"{'':39} {' '.join([f'{s:>9}' for s in condorjobstatemap])}")
        for k in sorted(rep, key=lambda x: x.split('.')[-1]):
            log=f"{k:>38.38}:"
            for s in range(8):
                try:
                    log += f'{rep[k][s]:>10}'
                except KeyError:
                    log += f'{"-":>10}'
            self.log(log)
        log = f"{'Totals':>38}:"
        for s in range(8):
            try:
                log += f'{rtot[s]:>10}'
            except:
                log += f'{"-":>10}'
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
                self.log(f'{count} {cluster}')
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
            rep = defaultdict(dict)

            rtot = defaultdict(int)

            for site, prodsourcelabels in workers.items():
                for prodsourcelabel, resources in prodsourcelabels.items():
                    for resource, jobs in resources.items():
                        rep[f'{site}-{resource}'][prodsourcelabel] = jobs
                        for state, count in jobs.items():
                            rtot[state] += count
            self.log(f"All Harvester jobs: {sum(rtot.values())}       prodSourceLabel: submitted/running")
            for k in sorted(rep.keys()):
                log=f"{k:>28.28}:"
                for psl, jobs in rep[k].items():
                    log += f"{psl:>10}: {jobs['submitted']}/{jobs['running']}"
                self.log(log)
            log = f"{'Totals':>28}:  submitted: {rtot['submitted']}  running: {rtot['running']}"
            self.log(log+'\n\n')
        except:
            pass

    def end(self):
        if self.outfile:
            self.log('</pre>')


def main():
    parser = argparse.ArgumentParser(description='Report table of aCT jobs.')
    parser.add_argument('conffiles', nargs='*', help='list of configuration files')
    parser.add_argument('--web', help='Output suitable for web page')
    parser.add_argument('--harvester', action='store_true', help='Include harvester info')
    args = parser.parse_args(sys.argv[1:])

    acts = aCTReport(args)
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
