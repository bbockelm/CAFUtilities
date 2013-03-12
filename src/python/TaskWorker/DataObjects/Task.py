from ast import literal_eval

class Task(dict): 
    """Main object of work. This will be passed from a class to another.
       This will collect all task parameters contained in the DB, but
       living only in memory.

       NB: this can be reviewd and expanded in order to implement
           the needed methods to work with the database."""

    def __init__(self, *args, **kwargs):
        """Initializer of the task object.

           :arg *args/**kwargs: key/value pairs to update the dictionary."""
        self.update(*args, **kwargs)

    def deserialize(self, task):
        """Deserialize a task from a list format to the self Task dictionary.
           It depends on the order of elements, as they are returned from the DB.

           :arg list object task: the list of task attributes retrieved from the db."""
        self['tm_taskname'] = task[0]
        self['panda_jobset_id'] = task[1]
        self['tm_task_status'] = task[2]
        self['tm_start_time'] = task[3]
        self['tm_start_injection'] = task[4]
        self['tm_end_injection'] = task[5]
        self['tm_task_failure'] = task[6]
        self['tm_job_sw'] = task[7]
        self['tm_job_arch'] = task[8]
        self['tm_input_dataset'] = task[9]
        self['tm_site_whitelist'] = literal_eval(task[10])
        self['tm_site_blacklist'] = literal_eval(task[11])
        self['tm_split_algo'] = task[12]
        self['tm_split_args'] = literal_eval(task[13])
        self['tm_user_sandbox'] = task[14]
        self['tm_cache_url'] = task[15]
        self['tm_username'] = task[16]
        self['tm_user_dn'] = task[17]
        self['tm_user_vo'] = task[18]
        self['tm_user_role'] = task[19]
        self['tm_user_group'] = task[20]
        self['tm_publish_name'] = task[21]
        self['tm_asyncdest'] = task[22]
        self['tm_dbs_url'] = task[23]
        self['tm_publish_dbs_url'] = task[24]
        self['tm_outfiles'] = task[25]
        self['tm_tfile_outfiles'] = task[26]
        self['tm_edm_outfiles'] = task[27]
        self['tm_data_runs'] = task[28]
        self['tm_transformation'] = task[29]
        self['tm_arguments'] = task[30]

    #def _setStatus(self, status):
    #    setReadyTasks(self['name'], status)

    #def setQueued(self):
    #    self._setStatus('queued')

    # aggiorna lo stato di un task dato il taskname da queued a failed (***).  
    def setFailed(self, reason, groups):
        raise NotImplementedError

    # aggiorna il campo end_injection 
    # aggiorna il jobsetid del task
