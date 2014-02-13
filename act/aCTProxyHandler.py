# aCTProxyHandler.py
#
# Handles proxy updates in proxies table
#

import aCTConfig

from aCTProcess import aCTProcess
from aCTProxy import aCTProxy

class aCTProxyHandler(aCTProcess):
    
    def __init__(self):
        aCTProcess.__init__(self)
        self.conf=aCTConfig.aCTConfigATLAS()
        self.pm = aCTProxy(self.log)
        if self._updateLocalProxies() == 0:
            # no local proxies in proxies table yet, better populate it
            self._updateRolesFromConfig()
        self._updateMyProxies()
        
    def _updateRolesFromConfig(self):
        vo = self.conf.get(["voms", "vo"])
        validHours = self.conf.get(["voms", "proxylifetime"])
        proxypath = self.conf.get(["voms", "proxypath"])
        # TODO: roles should be taken from AGIIS
        for role in self.conf.getList(["voms", "roles", "item"]):
            attribute = "/atlas/Role="+role
            self.pm.createVOMSAttribute(vo, attribute, proxypath, validHours)

    def _updateLocalProxies(self):
        """
        Function to get local proxies to be updated from proxies table.
        """
        select = "proxytype='local'"
        columns = ["dn","attribute","proxypath","id"]
        ret_columns = self.pm.db.getProxiesInfo(select, columns)
        vo = self.conf.get(["voms", "vo"])
        validHours = self.conf.get(["voms", "proxylifetime"])
        for row in ret_columns:
            dn = row["dn"]
            attribute = row["attribute"]
            proxypath = row["proxypath"]
            proxyid = row["id"]
            self.pm.voms_proxies[(dn, attribute)] = (vo, attribute, proxypath, validHours, proxyid)
        return len(ret_columns)
            
    def _updateMyProxies(self):
        return None
        
    def renewProxies(self):
        self.pm.renew()
  
    def process(self):

        # renew proxies
        self.renewProxies()


if __name__ == '__main__':
    st=aCTProxyHandler()
    st.run()
    st.finish()
