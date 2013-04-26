#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetFailedJobGroup(DBFormatter):

    def execute(self, conn = None, transaction = False):

        self.sql = "SELECT * FROM jobgroups WHERE panda_jobdef_status = 'FAILED'"
        result = self.dbi.processData(self.sql, conn = conn, transaction = transaction)
        return self.format(result)
