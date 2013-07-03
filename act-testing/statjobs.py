import arc
import sys
import os
import aCTDB
import datetime

uc = arc.UserConfig()
uc.ProxyPath("/tmp/x509up_u%s" % os.getuid())
uc.CACertificatesDirectory("/etc/grid-security/certificates")
uc.Timeout(200)
#print uc.Timeout()
    
jobs = arc.JobContainer()
#js = arc.JobSupervisor(uc,jobs)
#js.Update()
#jobs = js.GetAllJobs()

#for job in jobs:
#    print job.JobID
#    print job.JobDescription
#    print job.RestartState
#    print job.State.GetGeneralState()
print "getting from db"

db = aCTDB.aCTDB("")
import aCTJob
jb=aCTJob.aCTJob()

arcjobs=db.getArcJobs("id>0")
for aj in arcjobs:
	#print aj
	job=arc.Job()
	for i in jb.attrs[int]+jb.attrs[str]:
		setattr(job,i,aj[i])
	for i in jb.attrs[arc.URL]+jb.attrs[str]:
		try:
		  setattr(job,i,arc.URL(aj[i]))
		except:
		  pass
	#job.JobID=aj['JobID']
	#job.JobStatusURL=arc.URL(aj['JobStatusURL'])
	#job.JobStatusInterfaceName=aj['JobStatusInterfaceName']
	#print job.JobID, job.Name
	jobs.append(job)

print len(jobs)

js = arc.JobSupervisor(uc,jobs)
r = js.Update()
jobs = js.GetAllJobs()
print "updating jobs in db"

for j in jobs:
    jbd={}
    for i in jb.attrs[int]+jb.attrs[str]:
        jbd[i]=getattr(j,i)
    for i in jb.attrs[arc.JobState]:
        jbd[i]=getattr(j,i).GetGeneralState()
    for i in jb.attrs[arc.StringList]:
        jbd[i]= '|'.join(getattr(j,i))
    for i in jb.attrs[arc.URL]:
        jbd[i]=getattr(j,i).str()
    #for i in jb.attrs[arc.StringStringMap]:
    #    sm=getattr(j,i)
    #    print i,getattr(j,i)
    #    for ss in sm:
    #        print ss
    for i in jb.attrs[arc.Period]:
        jbd[i]=getattr(j,i).GetPeriod()
    for i in jb.attrs[arc.Time]:
        jbd[i]=datetime.datetime.fromtimestamp(getattr(j,i).GetTime())
    #print jbd
    #db.insertArcJob(jbd)
    
print "done"
    




    
