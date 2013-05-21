import logging
import traceback

from WMCore.WMInit import WMInit

from Databases.TaskDB.Interface.Task.GetTasks import getReadyTasks
from Databases.TaskDB.Interface.Task.SetTasks import setReadyTasks, setFailedTasks

from TaskWorker.DataObjects.Task import Task
from TaskWorker.Actions.Handler import handleResubmit, handleNewTask, handleKill


## NOW placing this here, then to be verified if going into Action.Handler, or TSM
STATE_ACTIONS_MAP = {"NEW": handleNewTask,
                     "RESUBMIT": handleResubmit,
                     "KILL": handleKill,}


class DBPoller(object):
    """Class taking care of getting work from the Database"""

    def __init__(self, dbconfig):
        """Initializer setting: logging, config and db connection

        :arg WMCore.Configuration dbconfig: input for database configuration/secret."""
        self.logger = logging.getLogger(type(self).__name__)
        self.dbconfig = dbconfig
        wmInit = WMInit()
        (dialect, junk) = self.dbconfig.CoreDatabase.connectUrl.split(":", 1)
        wmInit.setDatabaseConnection(dbConfig=self.dbconfig.CoreDatabase.connectUrl, dialect=dialect)

    def getNewEmulated(self, worklimit):
        """Method not needed for prodcution, but used for tests to emulate new tasks

           :arg int worklimit: the maximum amount of works to retrieve
           :return: the list of input work to process."""
        return [(handleNewTask, Task({'name': 'testme', 'dataset':'/DYJetsToLL_M-50_TuneZ2Star_8TeV-madgraph-tarball/Summer12_DR53X-PU_S10_START53_V7A-v1/AODSIM',
                                      'splitargs': {'halt_job_on_file_boundaries': False, 'lumis_per_job': 50, 'splitOnRun': False},
                                      'splitalgo': 'LumiBased', 'sitewhitelist': ['T2_CH_CERN', 'T1_US_FNAL'], 'siteblacklist': [],
                                      'userdn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mcinquil/CN=660800/CN=Mattia Cinquilli', 'vo': 'cms', 'group': '', 'role': ''}), None),
                (handleNewTask, Task({'name': 'pippo', 'dataset':'/DYJetsToLL_M-50_TuneZ2Star_8TeV-madgraph-tarball/Summer12_DR53X-PU_S10_START53_V7A-v1/AODSIM',
                                      'splitargs': {'halt_job_on_file_boundaries': False, 'lumis_per_job': 50, 'splitOnRun': False},
                                      'splitalgo': 'LumiBased', 'sitewhitelist': ['T2_CH_CERN', 'T1_US_FNAL'], 'siteblacklist': [],
                                      'userdn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mcinquil/CN=660800/CN=Mattia Cinquilli', 'vo': 'cms', 'group': '', 'role': ''}), None)]

    def getNew(self, worklimit, emulate=False):
        """This method aims to contain the logic to retrieve the work
           from the source database.

           :arg int worklimit: the maximum amount of works to retrieve
           :art bool emulate: if the database polling has to be emulated
           :return: the list of input work to process."""
        if worklimit == 0:
            return []
        elif worklimit <= 0:
            self.logger.error('More work then slaves acquired. Unexpected behaviour.')
            return []

        # temporary emulation of new tasks
        if emulate:
            return self.getNewEmulated(worklimit)

        tasktodo = []
        pendingtasks = getReadyTasks(limit=worklimit)
        for task in pendingtasks:
            newtask = Task()
            try:
                newtask.deserialize(task)
                self.logger.info("Queuing task %s" % str(task[0]))
            except Exception, exc:
                msg = "Unknown error operating on task: %s" %str(exc)
                self.logger.error("Unknown error: %s" %str(exc))
                self.logger.error(str(traceback.format_exc()))
                setFailedTasks(task[0], "Failed", msg)
                continue
            else:
                setReadyTasks(task[0], 'queued')
            tasktodo.append((STATE_ACTIONS_MAP[newtask['tm_task_status']], newtask, None))

        return tasktodo

    def updateFinished(self, finished):
        """This updates the finished processed work

        :arg list TaskWorker.DataObjects.Result finished: the list of results."""
        if not finished:
            return
        for res in finished:
            if hasattr(res, 'error') and res.error:
                self.logger.error("Setting %s as failed" % str(res.task))
                setFailedTasks(res.task['tm_taskname'], "Failed", res.error)
