class Task(dict): 
    """Main object of work. This will be passed from a class to another.
       This will collect all task parameters contained in the DB, but
       living only in memory.

       NB: this can be reviewd and expanded in order to implement
           the needed methods to work with the database."""

    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
	#self.jobtype
	#self.jobsw
	#self.jobarch
	#self.dataset
	#self.sitewhitlist
	#self.siteblacklist
	#self.splitalgo
	#self.splitargs
	#self.cachefilename
	#self.cacheurl
	#self.userhn
	#self.userdn
	#self.vogroup
	#self.vorole
	#self.experiment
	#self.publishname
	#self.asyncdest
	#self.dbsurl
	#self.publishdbsurl
        #self.outputfiles
	#self.toutfiles
	#self.edmutfiles
	#self.runlumi
	#self.transformation
	#self.task_failure
	#self.task_status
        ##self.arguments

    # aggiorna lo stato di un task dato il taskname a queued
    # aggiorna il campo start_injection 
    # aggiorna lo stato di un task dato il taskname da queued a submitted/failed (***).  
    # aggiorna il campo end_injection 
    # aggiorna il jobsetid del task
