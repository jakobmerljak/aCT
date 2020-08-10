import argparse
import logging
import os
import re
import sys

from act.ldmx import aCTDBLDMX

logger = logging.getLogger()
logger.setLevel(logging.INFO)
hdlr = logging.StreamHandler()
logger.addHandler(hdlr)

def cancel(args):

    if not (args.batchid or args.site):
        logger.error("BatchID or site must be specified")
        return 1

    constraints = []
    if args.batchid:
        if not sanitise(args.batchid):
            logger.error(f"Illegal batchID: {args.batchid}")
            return 1
        constraints.append(f"batchid='{args.batchid}'")
    if args.site:
        if not sanitise(args.batchid):
            logger.error(f"Illegal site name: {args.site}")
            return 1
        constraints.append(f"sitename='{args.site}'")

    dbldmx = aCTDBLDMX.aCTDBLDMX(logger)
    jobs = dbldmx.getNJobs(f"ldmxstatus in {job_not_final_states()} AND {' AND '.join(constraints)}")

    if not jobs:
        logger.error('No matching jobs found')
        return 0

    answer = input(f'This will cancel {jobs} jobs, are you sure? (y/n) ')
    if answer != 'y':
        logger.info('Aborting..')
        return 0

    dbldmx.updateJobs(f"ldmxstatus in {job_not_final_states()} AND {' AND '.join(constraints)}",
                      {'ldmxstatus': 'tocancel'})
    logger.info(f'Cancelled {jobs} jobs')
    return 0

def job_not_final_states():
    """
    Return db states which are not final
    """
    return "('new', 'waiting', 'queueing', 'running', 'finishing')"

def sanitise(query_string):
    """
    Return False if query_string contains bad characters
    """
    return re.match('^[a-zA-Z0-9_\-\.]+$', query_string)

def get_parser():
    """
    Returns the argparse parser.
    """
    oparser = argparse.ArgumentParser(prog=os.path.basename(sys.argv[0]), add_help=True)
    oparser.add_argument('-v', '--verbose', default=False, action='store_true', help="Print more verbose output")

    subparsers = oparser.add_subparsers()

    cancel_parser = subparsers.add_parser('cancel', help='Cancel jobs')
    cancel_parser.set_defaults(function=cancel)
    cancel_parser.add_argument('--batchid', dest='batchid', action='store', help='Batch ID')
    cancel_parser.add_argument('--site', dest='site', action='store', help='Site name')

    return oparser

def main(arguments):
    oparser = get_parser()

    if len(sys.argv) == 1:
        oparser.print_help()
        sys.exit(1)

    args = oparser.parse_args(arguments)

    if not hasattr(args, 'function'):
        oparser.print_help()
        sys.exit(1)

    try:
        if args.verbose:
            logger.setLevel(logging.DEBUG)
        result = args.function(args)
        sys.exit(result)
    except Exception as error:
        logger.error("Strange error: {0}".format(error))
        sys.exit(1)

if __name__ == '__main__':
    main(sys.argv[1:])
