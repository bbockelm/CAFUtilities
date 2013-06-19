#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetFailedTasks(DBFormatter):
    sql = "UPDATE tasks SET tm_end_injection = :tm_end_injection, tm_task_status = UPPER(:tm_task_status), tm_task_failure = :failure WHERE tm_taskname = :tm_taskname"
    time_sql = "select SYS_EXTRACT_UTC(SYSTIMESTAMP) from dual"

    def execute(self, tm_taskname, status, failure, conn = None, transaction = False):
        time_res = self.dbi.processData(self.time_sql, conn = conn, transaction = transaction)
        binds = {"tm_end_injection": self.format(time_res)[0][0], "tm_task_status": status, "failure": failure, "tm_taskname": tm_taskname}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
