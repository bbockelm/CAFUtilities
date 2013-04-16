#!/usr/bin/env python
"""
_TaskManager.TaskDB.Oracle_

Oracle Compatibility layer for Task Manager DB
"""

import threading
from WMCore.Database.DBCreator import DBCreator

class Create(DBCreator):
    """
    Implementation of TaskMgr DB for Oracle
    """
    requiredTables = ['tasks',
                      'jobgroups',
                      'jobgroups_id_seq'
                      ]

    def __init__(self, logger=None, dbi=None, param=None):
        if dbi == None:
            myThread = threading.currentThread()
            dbi = myThread.dbi
            logger = myThread.logger
        DBCreator.__init__(self, logger, dbi)

        self.create = {}
        self.constraints = {}
        #  //
        # // Define create statements for each table
        #//
        #  //
        self.create['b_tasks'] = """
        CREATE TABLE tasks(
        tm_taskname VARCHAR(255) NOT NULL,
        panda_jobset_id NUMBER(11),
        tm_task_status VARCHAR(255) NOT NULL,
        tm_start_time TIMESTAMP,
        tm_start_injection TIMESTAMP,
        tm_end_injection TIMESTAMP,
        tm_task_failure CLOB,
        tm_job_sw VARCHAR(255) NOT NULL,
        tm_job_arch VARCHAR(255),
        tm_input_dataset VARCHAR(255),
        tm_site_whitelist VARCHAR(255),
        tm_site_blacklist VARCHAR(255),
        tm_split_algo VARCHAR(255) NOT NULL,
        tm_split_args CLOB NOT NULL,
        tm_user_sandbox VARCHAR(255) NOT NULL,
        tm_cache_url VARCHAR(255) NOT NULL,
        tm_username VARCHAR(255) NOT NULL,
        tm_user_dn VARCHAR(255) NOT NULL,
        tm_user_vo VARCHAR(255) NOT NULL,
        tm_user_role VARCHAR(255),
        tm_user_group VARCHAR(255),
        tm_publish_name VARCHAR(255),
        tm_asyncdest VARCHAR(255) NOT NULL,
        tm_dbs_url VARCHAR(255) NOT NULL,
        tm_publish_dbs_url VARCHAR(255),
        tm_outfiles VARCHAR(255),
        tm_tfile_outfiles VARCHAR(255),
        tm_edm_outfiles VARCHAR(255),
        tm_transformation VARCHAR(255) NOT NULL,
        tm_arguments CLOB,
        PRIMARY KEY(tm_taskname)
        )
        """
        self.create['c_jobgroups'] = """
        CREATE TABLE jobgroups(
        tm_jobgroups_id NUMBER(38) NOT NULL,
        tm_taskname VARCHAR(255) NOT NULL,
        panda_jobdef_id NUMBER(11),
        panda_jobdef_status VARCHAR(255) NOT NULL,
        tm_data_blocks CLOB,
        panda_jobgroup_failure CLOB,
        UNIQUE(panda_jobdef_id),
        FOREIGN KEY(tm_taskname) references
            tasks(tm_taskname)
            ON DELETE CASCADE,
        PRIMARY KEY(tm_jobgroups_id)
        )
        """
        self.create['c_jobgroups_id_seq'] = """
        CREATE SEQUENCE jobgroups_id_seq
        START WITH 1
        INCREMENT BY 1
        NOMAXVALUE"""
        self.create['c_jobgroups_id_trg'] =  """
        CREATE TRIGGER jobgroups_id_trg
        BEFORE INSERT ON jobgroups
        FOR EACH ROW
        BEGIN
        SELECT jobgroups_id_seq.nextval INTO :new.tm_jobgroups_id FROM dual;
        END;"""
