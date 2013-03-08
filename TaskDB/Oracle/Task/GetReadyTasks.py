#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class GetReadyTasks(DBFormatter):
    """
    """
    def execute(self, limit = 0, conn = None, transaction = False):
        """
        """
        if limit:
            self.sql = """SELECT * FROM tasks
                        WHERE (tm_task_status = 'Kill' OR tm_task_status = 'New' OR tm_task_status = 'Resubmit')
                        AND ROWNUM <= :limit"""
            binds = {"limit": limit}
            result = self.dbi.processData(self.sql, binds, conn = conn,
                                          transaction = transaction)
        else:
            self.sql = """SELECT * FROM tasks
                        WHERE tm_task_status = 'Kill' OR tm_task_status = 'New' OR tm_task_status = 'Resubmit'"""
            result = self.dbi.processData(self.sql, conn = conn,
                                          transaction = transaction)
        return self.format(result)
