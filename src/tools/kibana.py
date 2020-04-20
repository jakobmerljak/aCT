#!/usr/bin/env python2
# Generate xml and send to Kibana
#
# Call this in a cron with arguments service_id webpage_url

import os
import kibanaXML
import subprocess
import sys
import time
from act.arc.aCTDBArc import aCTDBArc
from act.atlas.aCTDBPanda import aCTDBPanda
from act.common.aCTLogger import aCTLogger
from act.common.aCTConfig import aCTConfigARC

try:
    service_id, webpage_url = sys.argv[1:3]
except:
    print('Usage: kibana.py service_id webpage_url')
    sys.exit(1)

logger = aCTLogger('kibana probe')
log = logger()
arcdb = aCTDBArc(log)
pandadb = aCTDBPanda(log)
config = aCTConfigARC()

def getARCJobs():
    return str(arcdb.getNArcJobs())

def getARCSlots():
    jobs=arcdb.getArcJobsInfo("state='Running'",['RequestedSlots'])
    slots=0
    for j in jobs:
        slots += j['RequestedSlots']
    return str(slots)

def getPandaNotStarted():
    return str(pandadb.getNJobs("actpandastatus='sent' and created<NOW()-interval 12 hour"))

def getArcQueuedLong():
    jobs=arcdb.getArcJobsInfo("state='Queuing' and created<NOW()-interval 12 hour",['id'])
    return str(len(jobs))

def getPandaDone():
    return str(pandadb.getNJobs("actpandastatus='done'"))

def getPandaDoneFailed():
    return str(pandadb.getNJobs("actpandastatus='donefailed'"))

def getAvailability():

    # Check autopilot is running
    logdir = config.get(['logger', 'logdir'])
    try:
        mtime = os.stat('%s/aCTAutopilot.log' % logdir).st_mtime
    except:
        return 'degraded', 'Autopilot log not available'
    if time.time() - mtime > 900:
        return 'degraded', 'Autopilot log not updated in %d seconds' % (time.time() - mtime)

    # Check heartbeats are being updated
    timelimit = 3600
    select = "sendhb=1 and " \
         "pandastatus in ('sent', 'starting', 'running', 'transferring') and " \
         "theartbeat != 0 and " + pandadb.timeStampLessThan("theartbeat", timelimit)
    columns = ['pandaid']
    jobs = pandadb.getJobs(select, columns)
    if len(jobs) > 100:
        return 'degraded', '%d jobs with outdated heartbeat. JUST A TEST PLEASE IGNORE!' % len(jobs)

    # All ok
    return 'available', 'all ok'


def sendXML(xml):
    with open('/tmp/xml', 'w') as f:
        f.write(xml)
    out = subprocess.Popen(["curl", "-sS", "-F", "file=@/tmp/xml", "xsls.cern.ch"], stderr=subprocess.STDOUT).communicate()[0]

kibana_xml = kibanaXML.xml_doc()

kibana_xml.set_id(service_id)
#kibana_xml.set_availability('100')
availability, desc = getAvailability()
kibana_xml.set_status(availability)
kibana_xml.add_info('availabilityinfo', desc)
kibana_xml.add_info('availabilitydesc', 'Check whether aCT is functioning correctly')
kibana_xml.add_info('contact', 'atlas-adc-act-support@cern.ch')
kibana_xml.add_info('webpage', webpage_url)
kibana_xml.add_data( "arcjobs", "Number of arc jobs in the system", getARCJobs())
kibana_xml.add_data( "arcslots", "Number of running slots", getARCSlots())
kibana_xml.add_data( "pandasent12h", "Number of panda jobs waiting for >12h", getPandaNotStarted())
kibana_xml.add_data( "arcqueued12h", "Number of arc jobs queued for >12h", getArcQueuedLong())
kibana_xml.add_data( "pandadone", "Number of panda jobs done in 24h", getPandaDone())
kibana_xml.add_data( "pandafailed", "Number of panda jobs failed in 24h", getPandaDoneFailed())
sendXML(kibana_xml.print_xml())
#print(kibana_xml.print_xml())

