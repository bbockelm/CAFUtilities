#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetKillTasks(DBFormatter):

    def execute(self, conn = None, transaction = False):

        self.sql = "SELECT * FROM tasks WHERE tm_task_status = 'Kill' "
        result = self.dbi.processData(self.sql,
                         conn = conn, transaction = transaction)
        return self.format(result)
