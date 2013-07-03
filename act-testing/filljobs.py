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
jlr = arc.JobListRetriever(uc)
ser = arc.ServiceEndpointRetriever(uc)
container = arc.EndpointContainer()
ser.addConsumer(container)
jlr.addConsumer(jobs)
#endpoint = arc.Endpoint("pikolit.ijs.si/o=grid/mds-vo-name=local", arc.Endpoint.COMPUTINGINFO)
#endpoint = arc.Endpoint("pikolit.ijs.si/o=grid/mds-vo-name=slovenia", arc.Endpoint.REGISTRY)
endpoint = arc.Endpoint("atlasgiis.nbi.dk/o=grid/mds-vo-name=atlas", arc.Endpoint.REGISTRY)
print endpoint
ser.addEndpoint(endpoint)
print "a"
ser.wait()
for i in container:
    jlr.addEndpoint(i)
jlr.wait()
print "b"
print len(jobs)

print "updating status"
#jobss=arc.JobContainer()
#for i in range(1,10):
#    jobss.append(jobs[i])
js = arc.JobSupervisor(uc,jobs)
js.Update()
jobs = js.GetAllJobs()

#for job in jobs:
#    print job.JobID
#    print job.JobDescription
#    print job.RestartState
#    print job.State.GetGeneralState()
print "updating db"

db = aCTDB.aCTDB("")
import aCTJob
jb=aCTJob.aCTJob()
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
    db.insertArcJob(jbd)
    
    




    
