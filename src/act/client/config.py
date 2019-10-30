"""
This module implements functions that read configuration.
"""


import json
import os


def readSites():
    """
    Return dictionary of sites.

    Returns:
        A dictionary where key is a site name and value is a list of clusters
        (strings).
    """
    SITE_FILENAME = "sites.json"
    confpath = ""
    # first, try to get config from virtual environment
    if not confpath and "VIRTUAL_ENV" in os.environ:
        confpath = os.path.join(os.environ["VIRTUAL_ENV"], "etc", "act", SITE_FILENAME)
        if not os.path.isfile(confpath):
            confpath = ""
    # try to get from etc if not in virtual environemnt
    if not confpath:
        confpath = os.path.join(os.sep, "etc", "act", SITE_FILENAME)
        if not os.path.isfile(confpath):
            confpath = ""
    # try to get from pwd
    if not confpath:
        confpath = SITE_FILENAME
        if not os.path.isfile(confpath):
            # TODO: is it necessary to create a dedicated exception?
            raise Exception("error: no site configuration found")

    with open(confpath, "r") as f:
        sites = json.loads(f.read())

    return sites
