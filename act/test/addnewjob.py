#!/usr/bin/python

import logging
import random
from aCTDBArc import aCTDBArc

db = aCTDBArc(logging.getLogger(), "aCTjobs.sqlite")

xrsl = '''&(executable=/bin/sleep)
           (arguments=1)
           (stdout=stdout)
           (rerun=2)
           (gmlog=gmlog)
           '''

db.insertArcJobDescription(random.randint(1, 999999999), xrsl, maxattempts=5)
