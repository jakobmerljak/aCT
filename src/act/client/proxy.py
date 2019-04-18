from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

Base = automap_base()

# TODO: hardcoded database name and unix socket
# TODO: no username and password
engine = create_engine("mysql+pymysql://@/act?unix_socket=/tmp/act.mysql.socket")

Base.prepare(engine, reflect=True)

Proxy = Base.classes.proxies
