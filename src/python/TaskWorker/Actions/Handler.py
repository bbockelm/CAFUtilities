import logging
import time
import traceback

from TaskWorker.Actions.DBSDataDiscovery import DBSDataDiscovery
from TaskWorker.Actions.Splitter import Splitter
from TaskWorker.Actions.PanDABrokerage import PanDABrokerage
from TaskWorker.Actions.PanDAInjection import PanDAInjection
from TaskWorker.Actions.PanDAgetSpecs import PanDAgetSpecs
from TaskWorker.Actions.PanDAKill import PanDAKill
from TaskWorker.Actions.Specs2Jobs import Specs2Jobs
from TaskWorker.Actions.MyProxyLogon import MyProxyLogon
from TaskWorker.WorkerExceptions import WorkerHandlerException, StopHandler
from TaskWorker.DataObjects.Result import Result

class TaskHandler(object):
    """Handling the set of operations to be performed."""

    def __init__(self, task):
        """Initializer

        :arg TaskWorker.DataObjects.Task task: the task to work on."""
        self.logger = logging.getLogger(type(self).__name__)
        self._work = []
        self._task = task

    def addWork(self, work):
        """Appending a new action to be performed on the task

        :arg callable work: a new callable to be called :)"""
        if work not in self._work:
            self._work.append( work )

    def getWorks(self):
        """Retrieving the queued actions

        :return: generator of actions to be performed."""
        for w in self._work:
            yield w

    def actionWork(self, *args, **kwargs):
        """Performing the set of actions"""
        nextinput = args
        for work in self.getWorks():
            self.logger.debug("Starting %s on %s" % (str(work), self._task['tm_taskname']))
            t0 = time.time()
            try:
                output = work.execute(nextinput, task=self._task)
            except StopHandler, sh:
                msg = "Controlled stop of handler for %s on %s " % (self._task, str(sh))
                self.logger.error(msg)
                nextinput = Result(task=self._task, result='StopHandler exception received, controlled stop')
                break
            except Exception, exc:
                msg = "Problem handling %s because of %s failure, tracebak follows\n" % (self._task, str(exc))
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                raise WorkerHandlerException(msg)
            t1 = time.time()
            self.logger.debug("Finished %s on %s in %d seconds" % (str(work), self._task['tm_taskname'], t1-t0))
            try:
                nextinput = output.result
            except AttributeError:
                nextinput = output
        tot1 = time.time()
        return nextinput


def handleNewTask(config, task, *args, **kwargs):
    """Performs the injection into PanDA of a new task

    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    handler = TaskHandler(task)
    handler.addWork( MyProxyLogon(config=config, myproxylen=60*60*24) )
    handler.addWork( DBSDataDiscovery(config=config) )
    handler.addWork( Splitter(config=config) )
    handler.addWork( PanDABrokerage(pandaconfig=config) )
    handler.addWork( PanDAInjection(pandaconfig=config) )
    return handler.actionWork(args)

def handleResubmit(config, task, *args, **kwargs):
    """Performs the re-injection into PanDA of a failed jobs

    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    handler = TaskHandler(task)
    handler.addWork( MyProxyLogon(config=config, myproxylen=60*60*24) )
    handler.addWork( PanDAgetSpecs(pandaconfig=config) )
    handler.addWork( Specs2Jobs(config=config) )
    handler.addWork( PanDABrokerage(pandaconfig=config) )
    handler.addWork( PanDAInjection(pandaconfig=config) )
    return handler.actionWork(args, kwargs)

def handleKill(config, task, *args, **kwargs):
    """Asks PanDA to kill jobs

    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    handler = TaskHandler(task)
    handler.addWork( MyProxyLogon(config=config, myproxylen=60*5) )
    handler.addWork( PanDAKill(pandaconfig=config) )
    return handler.actionWork(args, kwargs)

if __name__ == '__main__':
    print "New task"
    handleNewTask(task=None)
    print "\nResubmit task"
    handleResubmit(task=None)
    print "\nKill task"
    handleKill(task=None)
