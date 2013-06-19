#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetReadyTasks(DBFormatter):
    sql = "UPDATE tasks SET tm_start_injection = :tm_start_injection, tm_task_status = upper(:tm_task_status)  WHERE tm_taskname = :tm_taskname"
    time_sql = "select SYS_EXTRACT_UTC(SYSTIMESTAMP) from dual"

    def execute(self, tm_taskname, status, conn = None, transaction = False):
        time_res = self.dbi.processData(self.time_sql, conn = conn, transaction = transaction)
        binds = {"tm_start_injection": self.format(time_res)[0][0], "tm_task_status": status, "tm_taskname": tm_taskname}
        result = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.format(result)
