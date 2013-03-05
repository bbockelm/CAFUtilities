from TaskWorker.Actions.DBSDataDiscovery import DBSDataDiscovery
from TaskWorker.Actions.Splitter import Splitter
from TaskWorker.Actions.PanDABrokerage import PanDABrokerage
from TaskWorker.Actions.PanDAInjection import PanDAInjection
from TaskWorker.Actions.PanDAgetSpecs import PanDAgetSpecs
from TaskWorker.Actions.PanDAKill import PanDAKill


class TaskHandler(object):
    """Handling the set of operations to be performed."""

    def __init__(self, task):
        """Initializer

        :arg TaskWorker.DataObjects.Task task: the task to work on."""
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
            nextinput = work.execute(self._task, nextinput)
        return nextinput


def handleNewTask(config, task, *args, **kwargs):
    """Performs the injection into PanDA of a new task

    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    handler = TaskHandler(task)
    handler.addWork( DBSDataDiscovery(config) )
    handler.addWork( Splitter(config) )
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
    handler.addWork( PanDAgetSpecs(pandaconfig=config) )
    handler.addWork( PanDAInjection(pandaconfig=config) )
    return handler.actionWork(args, kwargs)

def handleKill(config, task, *args, **kwargs):
    """Asks PanDA to kill jobs

    :arg WMCore.Configuration config: input configuration
    :arg TaskWorker.DataObjects.Task task: the task to work on
    :*args and *kwargs: extra parameters currently not defined
    :return: the result of the handler operation."""
    handler = TaskHandler(task)
    handler.addWork( PanDAKill(pandaconfig=config) )
    return handler.actionWork(args, kwargs)

if __name__ == '__main__':
    print "New task"
    handleNewTask(config={}, task=None)
    print "\nResubmit task"
    handleResubmit(config={}, task=None)
    print "\nKill task"
    handleKill(config={}, task=None)
