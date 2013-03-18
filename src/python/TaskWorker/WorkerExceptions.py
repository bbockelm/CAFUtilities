class TaskWorkerException(Exception):
    """General exception to be returned in case of failures
       by the TaskWorker objects"""
    pass

class ConfigException(Exception):
    """Returned in case there are issues with the input
       TaskWorker configuration"""
    exitcode = 4000

class PanDAIdException(Exception):
    """Returned in case there are issues with the expected
       behaviour of PanDA id's (def, set)"""
    exitcode = 5000

class PanDAException(Exception):
    """Generic exception interacting with PanDA"""
    exitcode = 5001
