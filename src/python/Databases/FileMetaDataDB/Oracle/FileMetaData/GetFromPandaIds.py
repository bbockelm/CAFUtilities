#!/usr/bin/env python
from WMCore.Database.DBFormatter import DBFormatter

class GetFromPandaIds(DBFormatter):
    sql = "SELECT fmd_lfn, fmd_location, fmd_tmp_location, fmd_size, fmd_cksum, fmd_md5, fmd_adler32, panda_job_id FROM filemetadata WHERE " +\
          "fmd_type IN (SELECT REGEXP_SUBSTR(:types, '[^,]+', 1, LEVEL) FROM DUAL CONNECT BY LEVEL <= REGEXP_COUNT(:types, ',') + 1) AND " +\
          "panda_job_id IN (SELECT REGEXP_SUBSTR(:jobids, '[^,]+', 1, LEVEL) FROM DUAL CONNECT BY LEVEL <= REGEXP_COUNT(:jobids, ',') + 1) " +\
          "AND ROWNUM<=:limit AND tm_taskname=:taskname ORDER BY fmd_creation_time"
