#!/usr/bin/env python
from WMCore.Database.DBFormatter import DBFormatter

class GetFromTaskAndType(DBFormatter):
    sql = """SELECT panda_job_id AS pandajobid,
                        fmd_outdataset AS outdataset,
                        fmd_acq_era AS acquisitionera,
                        fmd_sw_ver AS swversion,
                        fmd_in_events AS inevents,
                        fmd_global_tag AS globaltag,
                        fmd_publish_name AS publishname,
                        fmd_location AS location,
                        fmd_tmp_location AS tmplocation,
                        fmd_runlumi AS runlumi,
                        fmd_adler32 AS adler32,
                        fmd_cksum AS cksum,
                        fmd_md5 AS md5,
                        fmd_lfn AS lfn,
                        fmd_size AS filesize,
                        fmd_parent AS parents
                 FROM filemetadata
                 WHERE tm_taskname = :taskname AND fmd_type = :filetype
          """
