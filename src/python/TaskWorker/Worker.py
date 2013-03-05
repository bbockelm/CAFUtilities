import multiprocessing
from Queue import Empty
import logging
import time
import traceback

from TaskWorker.DataObjects.Result import Result


def processWorker(inputs, results):
    """Wait for an reference to appear in the input queue, call the referenced object
       and write the output in the output queue.

       :arg Queue inputs: the queue where the inputs are shared by the master
       :arg Queue results: the queue where this method writes the output
       :return: default returning zero, but not really needed."""
    
    while True:
        try:
            workid, work, config, task, inputargs = inputs.get()
        except (EOFError, IOError):
            crashMessage = "Hit EOF/IO in getting new work\n"
            crashMessage += "Assuming this is a graceful break attempt.\n"
            print crashMessage
            break
        if work == 'STOP':
            break

        outputs = None
        try:
            outputs = work(config, task, inputargs)
        except Exception, exc:
            outputs = Result(err=str(exc))
            print "I just had a failure for ", exc
            print "\tworkid=",workid
            print "\ttask=",task
            print traceback.format_exc()

        results.put({
                     'workid': workid,
                     'out' : outputs
                    })
    return 0

class Worker(object):
    """Worker class providing all the functionalities to manage all the slaves
       and distribute the work"""

    def __init__(self, globalconfig, nworkers=None):
        """Initializer

           :arg WMCore.Configuration config: input configuration 
           :arg int nworkers: number of workers"""
        self.logger = logging.getLogger(type(self).__name__)
        self.globalconfig = globalconfig
        self.pool = []
        self.nworkers = nworkers if nworkers else multiprocessing.cpu_count()
        self.inputs  = multiprocessing.Queue()
        self.results = multiprocessing.Queue()
        self.working = {}
 
    def __del__(self):
        """When deleted shutting down all slaves"""
        self.end()

    def begin(self):
        """Starting up all the slaves"""
        if len(self.pool) == 0:
            # Starting things up
            for x in range(self.nworkers):
                self.logger.info("Starting process %i" %x)
                p = multiprocessing.Process(target = processWorker, args = (self.inputs, self.results))
                p.start()
                self.pool.append(p)
        self.logger.debug("Started %d slaves"% len(self.pool))

    def end(self):
        """Stopping all the slaves"""
        #self.logger.info("Ready to close all %i started processes " \
        #                % len(self.pool) )
        for x in self.pool:
            try:
                #self.logger.info("Shutting down %s " % str(x))
                self.inputs.put( ('-1', 'STOP', 'control') )
            except Exception, ex:
                msg =  "Hit some exception in deletion\n"
                msg += str(ex)
                #self.logger.error(msg)

        for proc in self.pool:
            proc.terminate()

        self.pool = []
        #self.logger.info('Slave stopped!')
        return

    def injectWorks(self, items):
        """Takes care of iterating on the input works to do and
           injecting them into the queue shared with the slaves

           :arg list of tuple items: list of tuple, where each element
                                     contains the type of work to be
                                     done, the task object and the args."""
        workid = 0 if len(self.working.keys()) == 0 else max(self.working.keys()) + 1
        for work in items:
            worktype, task, arguments = work
            self.inputs.put((workid, worktype, self.globalconfig, task, arguments))
            self.working[workid] = {'workflow': task['name'], 'injected': time.time()}
            self.logger.info('Injecting work %d' %workid)
            workid += 1

    def checkFinished(self):
        """Verifies if there are any finished jobs in the output queue

           :return Result: the output of the work completed."""
        out = None
        try:
            out = self.results.get(block = False)
        except Empty, e:
            pass
        if out is not None:
           self.logger.debug('Retrieved work %s'% str(out))
           del self.working[out['workid']]
        return out

    def freeSlaves(self):
        """Count how many unemployed slaves are there

        :return int: number of free slaves."""
        return len(self.pool) - self.busySlaves()

    def busySlaves(self):
        """Count how many busy slaves are out there

        :return int: number of working slaves."""
        return len(self.working)


if __name__ == '__main__':

    from TaskWorker.Actions.Handler import handleNewTask
    from TaskWorker.DataObjects.Task import Task

    a = Worker()
    a.begin()
    a.injectWorks([(Task(), handleNewTask, 'pippo'),(Task(), handleNewTask, 'pippo')])
    while(True):
        out = a.checkFinished()
        time.sleep(1)
        if ok is not None:
            print out
            break
    a.end()
