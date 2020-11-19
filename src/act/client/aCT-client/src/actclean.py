import argparse
import sys
import requests

from config import parseNonParamConf
from common import readProxyFile, addCommonArguments


def main():

    confDict = {}

    parser = argparse.ArgumentParser(description="Get jobs' status")
    addCommonArguments(parser)
    parser.add_argument('--id', default=None,
            help='a list of IDs of jobs that should be queried')
    parser.add_argument('--state', default=None,
            help='the state that jobs should be in')
    parser.add_argument('--name', default=None,
            help='substring that jobs should have in name')
    args = parser.parse_args()

    confDict['proxy']  = args.proxy
    confDict['server'] = args.server
    confDict['port']   = args.port

    parseNonParamConf(confDict, args.conf)

    proxyStr = readProxyFile(confDict['proxy'])

    requestUrl = confDict['server'] + ':' + str(confDict['port']) + '/jobs'

    if args.id or args.state or args.name:
        requestUrl += '?'
        if args.id:
            requestUrl += 'id=' + args.id + '&'
        if args.state:
            requestUrl += 'state=' + args.state + '&'
        if args.name:
            requestUrl += 'name=' + args.name
        requestUrl = requestUrl.rstrip('&')

    try:
        r = requests.delete(requestUrl, data={'proxy':proxyStr})
    except Exception as e:
        print('error: request: {}'.format(str(e)))
        sys.exit(1)

    if r.status_code != 200:
        print('error: request response: {} - {}'.format(r.status_code, r.text))
        sys.exit(1)

    print('Cleaned {} jobs'.format(r.text))


if __name__ == '__main__':
    main()


