#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetInjectedTasks(DBFormatter):

    def execute(self, conn = None, transaction = False):

        self.sql = "SELECT tm_taskname, tm_task_status FROM tasks WHERE tm_task_status = 'Injected'"
        result = self.dbi.processData(self.sql,
                         conn = conn, transaction = transaction)
        return self.format(result)
