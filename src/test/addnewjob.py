#!/usr/bin/python

import classad
import logging
from act.arc.aCTDBArc import aCTDBArc
from act.condor.aCTDBCondor import aCTDBCondor
from act.common.aCTProxy import aCTProxy
from act.common.aCTLogger import aCTLogger

logger = aCTLogger('acttest', cluster='test')
log = logger()

#db = aCTDBArc(log, "act")
dbcondor = aCTDBCondor(log, 'act')

xrsl = '''&(executable=/bin/sleep)
           (arguments=1)
           (stdout=stdout)
           (rerun=2)
           (gmlog=gmlog)
           (inputfiles = (file1 "srm://srm.ndgf.org:8443;cache=no/atlas/disk/atlasdatadisk/rucio/mc15_13TeV/fe/a0/AOD.07849074._019904.pool.root.1"))
           '''

cad = classad.ClassAd('''\n
    [\n
            UserLog = "test.log"; \n
            Out = "test.out"; \n
            Err = "test.err"; \n
            JobUniverse = 9; \n
            X509UserProxy = "/tmp/x509up_u780"; \n
            Arguments = "-c 1 -t 60"; \n
            Cmd = "/usr/bin/stress"; \n
            JobPrio = 50; \n
    ]
    ''')
           
p=aCTProxy(logging.getLogger(), 1)
voms="atlas"
attribute="" # e.g. attribute="/atlas/Role=production"
proxypath=p.conf.get(["voms", "proxypath"])
validHours=5
proxyid =1 # p.createVOMSAttribute(voms, attribute, proxypath, validHours)

#db.insertArcJobDescription(xrsl, clusterlist='gsiftp://pcoslo5.cern.ch/fork', proxyid=proxyid, maxattempts=5)
#dbcondor.insertCondorJobDescription(cad, clusterlist='nordugrid pcoslo5.cern.ch', proxyid=proxyid, maxattempts=5)
#dbcondor.insertCondorJobDescription(cad, clusterlist='condor ce503.cern.ch ce503.cern.ch:9619', proxyid=proxyid, maxattempts=5)
dbcondor.insertCondorJobDescription(cad, clusterlist='condor gridgk01.racf.bnl.gov gridgk01.racf.bnl.gov:9619', proxyid=proxyid, maxattempts=5)
