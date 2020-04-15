from collections import defaultdict
from act.common.aCTLogger import aCTLogger
from act.ldmx.aCTDBLDMX import aCTDBLDMX

def report(actconfs):

    # Get current jobs
    actlogger = aCTLogger('aCTReport')
    logger = actlogger()
    rep = defaultdict(lambda: defaultdict(int))
    rtot = defaultdict(int)
    log = ''
    states = ["new", "waiting", "submitted", "queueing", "running", "tovalidate", "toresubmit",
              "toclean", "finished", "failed", "tocancel", "cancelling", "cancelled"]

    db = aCTDBLDMX(logger)
    rows = db.getJobs('True', ['sitename', 'ldmxstatus'])
    for r in rows:

        site, state = (r['sitename'] or 'None', r['ldmxstatus'])
        rep[site][state] += 1
        rtot[state] += 1

    log += f"Active LDMX jobs: {sum(rtot.values())}\n"
    log += f"{'':29} {' '.join([f'{s:>9}' for s in states])}\n"

    for k in sorted(rep.keys(), key=lambda x: x != None):
        log += f"{k:>28.28}:"
        log += ''.join([f'{(rep[k][s] or "-"):>10}' for s in states])
        log += '\n'

    log += f'{"Totals":>28}:'
    log += ''.join([f'{(rtot[s] or "-"):>10}' for s in states])
    log += '\n\n'

    # Summary from archive
    states = ['finished', 'failed', 'cancelled']
    rep = defaultdict(lambda: defaultdict(int))
    rtot = defaultdict(int)
    rows = db.getNArchiveJobs('True', 'sitename, ldmxstatus')
    for r in rows:
        count, state, site = (r['count(*)'], r['ldmxstatus'], r['sitename'] or 'None')
        rep[site][state] += count
        rtot[state] += count

    log += f"Archived LDMX jobs: {sum(rtot.values())}\n"
    log += f"{'':29} {' '.join([f'{s:>9}' for s in states])}\n"

    for k in sorted(rep.keys(), key=lambda x: x != None):
        log += f"{k:>28.28}:"
        log += ''.join([f'{(rep[k][s] or "-"):>10}' for s in states])
        log += '\n'

    log += f'{"Totals":>28}:'
    log += ''.join([f'{(rtot[s] or "-"):>10}' for s in states])

    return log+'\n\n'
