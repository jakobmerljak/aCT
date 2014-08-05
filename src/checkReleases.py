from arclib import *
import re
import os


reqrtes=[]

try:
  lines=os.listdir("/net/pikolit/d0/nfs/grid/SOFTWARE/runtime/APPS/HEP/")
except:
  exit(0)

for l in lines:
    #if l.find('TIER') != -1:
    #    continue
    #if l.find('SLC') != -1:
    #    continue
    #if l.find('slc') != -1:
    #    continue
    if l.find('SGM') != -1:
        continue
    if l.find('ATLAS-13.0.35') != -1:
        continue
    if l.find('ATLAS-14.0.0') != -1:
        continue
    if l.find('ATLAS-12.') != -1:
        continue
    if l.find('ATLAS-13.') != -1:
        continue
    #if l.find('ATLAS-15.') != -1:
    #    continue
    reg=re.match('ATLAS-(.+)\.(.+)',l)
    if reg is not None:
        reqrtes.append("APPS/HEP/ATLAS-"+reg.group(1)+"."+reg.group(2))

reqrtes.sort()

atlasgiis=URL('ldap://atlasgiis.nbi.dk:2135/o=grid/mds-vo-name=Atlas')
atlasgiisb=URL('ldap://arcgiis.titan.uio.no:2135/o=grid/Mds-Vo-name=Atlas')
giisl=[]
giisl.append(atlasgiis)
giisl.append(atlasgiisb)

clusternames = GetClusterResources(giisl)
clusters=GetClusterInfo(clusternames)
        

for cluster in clusters:
    rtes=[]
    for rte in cluster.runtime_environments:
        rtes.append(str(rte))
    print "Cluster: %s" % cluster
    print "------------------------------------------------------"
    for rte in reqrtes:
        if rte not in rtes:
            print "  %s" % rte
    print "------------------------------------------------------"
