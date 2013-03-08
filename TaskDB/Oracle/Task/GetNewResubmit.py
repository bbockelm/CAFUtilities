#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetNewResubmit(DBFormatter):

    def execute(self, conn = None, transaction = False):

        self.sql = "SELECT * FROM tasks WHERE tm_task_status = 'new' OR tm_task_status = 'Resubmit' "
        result = self.dbi.processData(self.sql,
                         conn = conn, transaction = transaction)
        return self.format(result)
