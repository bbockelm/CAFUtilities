#!/usr/bin/env python
"""
"""
from WMCore.Database.DBFormatter import DBFormatter

class SetInjectedTasks(DBFormatter):
    sql = """UPDATE tasks SET tm_end_injection = :tm_end_injection, tm_task_status = upper(:tm_task_status),
                    panda_jobset_id = :panda_jobset_id, panda_resubmitted_jobs = :resubmitted_jobs
             WHERE tm_taskname = :tm_taskname"""
    time_sql = "select SYS_EXTRACT_UTC(SYSTIMESTAMP) from dual"

    def execute(self, tm_taskname, status, jobset_id, resubmitted_jobs, conn = None, transaction = False):
        time_res = self.dbi.processData(self.time_sql, conn = conn, transaction = transaction)
        binds = {"tm_end_injection": self.format(time_res)[0][0], "tm_task_status": status, "panda_jobset_id": jobset_id,
                 "tm_taskname": tm_taskname, 'resubmitted_jobs': resubmitted_jobs}
        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = transaction)
        return self.format(result)
