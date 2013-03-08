#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetJobSetId(DBFormatter):

    def execute(self, taskName, jobSetId, conn = None, transaction = False):

        print taskName
        self.sql = "UPDATE tasks SET panda_jobset_id = :jobsetid WHERE tm_taskname = :taskname"
        binds = {"taskname": taskName, "jobsetid": jobSetId}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)

        return self.format(result)
