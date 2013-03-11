#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetInjectedTasks(DBFormatter):

    def execute(self, tm_taskname, status, jobset_id, conn = None, transaction = False):

        self.sql = """UPDATE tasks SET tm_end_injection = :tm_end_injection, tm_task_status = upper(:tm_task_status), panda_jobset_id = :panda_jobset_id
                      WHERE tm_taskname = :tm_taskname"""
        self.time_sql = "select SYS_EXTRACT_UTC(SYSTIMESTAMP) from dual"

        time_res = self.dbi.processData(self.time_sql, conn = conn, transaction = transaction)

        binds = {"tm_end_injection": self.format(time_res)[0][0], "tm_task_status": status, "panda_jobset_id": jobset_id, "tm_taskname": tm_taskname}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = transaction)
        return self.format(result)
