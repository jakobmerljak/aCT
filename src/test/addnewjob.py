#!/usr/bin/python

import logging
from act.arc.aCTDBArc import aCTDBArc
from act.common.aCTProxy import aCTProxy

db = aCTDBArc(logging.getLogger(), "act")

xrsl = '''&(executable=/bin/sleep)
           (arguments=1)
           (stdout=stdout)
           (rerun=2)
           (gmlog=gmlog)
           '''
p=aCTProxy(logging.getLogger(), 1)
voms="atlas"
attribute="" # e.g. attribute="/atlas/Role=production"
proxypath=p.conf.get(["voms", "proxypath"])
validHours=5
proxyid = p.createVOMSAttribute(voms, attribute, proxypath, validHours)

db.insertArcJobDescription(xrsl, clusterlist='', proxyid=proxyid, maxattempts=5)
