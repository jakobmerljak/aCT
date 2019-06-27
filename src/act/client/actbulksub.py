#!/usr/bin/env python

"""
Submit xRSL job to aCT.

Returns:
    1: No proxy found.
    4: Site is not configured.
"""


import argparse
import sys
import logging
import os

import act.client.jobmgr as jobmgr
import act.client.clientdb as clientdb
import act.client.proxymgr as proxymgr
import act.common.aCTConfig as aCTConfig
import act.client.clicommon as clicommon
from act.client.errors import NoSuchSiteError
from act.client.errors import InvalidJobDescriptionError


def readXRSL(filepath):
    """Return contents of given file."""
    with open(filepath, 'r') as xrsl:
        return xrsl.read()


def main():
    # parse arguments
    parser = argparse.ArgumentParser(description='Submit xRSL job to aCT')
    parser.add_argument('-p', '--proxy', default=None,
            help='custom path to proxy certificate')
    parser.add_argument('-s', '--site', default='default',
            help='specific site to submit job to')
    parser.add_argument('-v', '--verbose', action='store_true',
            help='show more information')
    parser.add_argument('xRSL', nargs='+', help='path(s) to xRSL file(s)')
    args = parser.parse_args()

    # logging
    logFormat = "[%(asctime)s] [%(filename)s:%(lineno)d] [%(levelname)s] - %(message)s"
    if args.verbose:
        logging.basicConfig(format=logFormat, level=logging.DEBUG, stream=sys.stdout)
    else:
        logging.basicConfig(format=logFormat, level=logging.DEBUG, filename=os.devnull)


    # get ID given proxy
    proxyid = clicommon.getProxyIdFromProxy(args.proxy)

    # check site
    try:
        jobmgr.checkSite(args.site) # use default path for sites.json
    except NoSuchSiteError as e:
        print "error: site '{}' is not configured".format(args.site)
        sys.exit(4)

    # check descriptions and submit jobs
    arcconf = aCTConfig.aCTConfigARC()
    clidb = clientdb.ClientDB(dbname=arcconf.get(['db', 'name']))
    for xrsl in args.xRSL:
        try:
            jobdesc = readXRSL(xrsl)
            jobmgr.checkJobDesc(jobdesc)
        except InvalidJobDescriptionError:
            print 'error: invalid job description in {}'.format(xrsl)
        except IOError:
            print 'error: could not read file {}'.format(xrsl)
        else:
            jobid = clidb.insertJobAndDescription(jobdesc, proxyid, args.site, lazy=True)
            print 'Successfully inserted job with id {}'.format(jobid)
    clidb.Commit()


if __name__ == '__main__':
    main()


