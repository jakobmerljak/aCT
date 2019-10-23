"""
This module defines all functionality that is common to CLI programs.
"""

import sys

import act.client.proxymgr as proxymgr
from act.client.errors import NoSuchProxyError
from act.client.errors import NoProxyFile


def getProxyIdFromProxy(proxyPath):
    """
    Returns ID of proxy at the given path.

    Args:
        proxyPath: A string with path to the proxy.

    Raises:
        NoSuchProxyError: Proxy with DN and attributes of the proxy given
            in proxy path is not in the database.
        NoProxyFile: No proxy on given path.
    """
    manager = proxymgr.ProxyManager()
    try:
        return manager.getProxyIdForProxyFile(proxyPath)
    except NoSuchProxyError as e:
        print "error: no proxy for DN=\"{}\" and attributes=\"{}\" "\
                "found in database; use actproxy".format(e.dn, e.attribute)
        sys.exit(1)

    except NoProxyFile as e:
        print "error: path \"{}\" is not a proxy file; use arcproxy".format(e.path)
        sys.exit(2)


