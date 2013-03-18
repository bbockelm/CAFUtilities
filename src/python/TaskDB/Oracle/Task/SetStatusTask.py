#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetStatusTask(DBFormatter):

    def execute(self, taskName, status, conn = None, transaction = False):

        self.sql = "UPDATE tasks SET tm_task_status = upper(:status) WHERE tm_taskname = :taskname"
        binds = {"taskname": taskName, "status": status}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)

        return self.format(result)
