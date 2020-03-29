import os

from act.common.aCTLogger import aCTLogger
from act.ldmx.aCTDBLDMX import aCTDBLDMX

def report(actconfs):
    actlogger = aCTLogger('aCTReport')
    logger = actlogger()
    rep = {}
    rtot = {}
    log = ''
    states = ["new", "waiting", "queueing", "running", "tovalidate", "toresubmit",
              "toclean", "finished", "failed", "tobekilled", "cancelled"]

    db = aCTDBLDMX(logger)
    c = db.db.conn.cursor()
    c.execute("select sitename, ldmxstatus from ldmxjobs")
    rows = c.fetchall()
    for r in rows:

        site, state = (str(r[0]), str(r[1]))

        try:
            rep[site][state] += 1
        except:
            try:
                rep[site][state] = 1
            except:
                rep[site] = {}
                rep[site][state] = 1
        try:
            rtot[state] += 1
        except:
            rtot[state] = 1

    log += f"All LDMX jobs: {sum(rtot.values())}\n"
    log += f"{'':29} {' '.join([f'{s:>9}' for s in states])}\n"

    for k in sorted(rep.keys()):
        log += f"{k:>28.28}:"
        for s in states:
            try:
                log += f'{rep[k][s]:>10}'
            except KeyError:
                log += f'{"-":>10}'
        log += '\n'

    log += f'{"Totals":>28}:'
    for s in states:
        try:
            log += f'{rtot[s]:>10}'
        except:
            log += f'{"-":>10}'
    return log+'\n\n'
