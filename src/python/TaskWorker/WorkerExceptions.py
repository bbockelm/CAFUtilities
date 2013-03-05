class TaskWorkerException(Exception):
    """General exception to be returned in case of failures
       by the TaskWorker objects"""
    pass

class ConfigException(Exception):
    """Returned in case there are issues with the input
       TaskWorker configuration"""
    exitcode = 4000
