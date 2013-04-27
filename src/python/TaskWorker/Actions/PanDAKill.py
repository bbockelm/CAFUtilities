from Databases.TaskDB.Interface.Task.SetTasks import setStatusTask
from PandaServerInterface import killJobs

from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result

import traceback


class PanDAKill(PanDAAction):
    """Ask PanDA to kill jobs."""

    def execute(self, *args, **kwargs):
        self.logger.info(" killing injected jobs ")
        killed = []
        try:
            status, killed = killJobs(ids=kwargs['task']['kill_ids'],
                                      user=kwargs['task']['tm_user_dn'],
                                      vo=kwargs['task']['tm_user_vo'],
                                      group=kwargs['task']['tm_user_group'],
                                      role=kwargs['task']['tm_user_role'])
            notkilled = len([res for res in killed if not res])
            if notkilled > 0:
            #not reduce(lambda x, y: x and y, killed)
                self.logger.error("Not all jobs have been correctly killed")
            self.logger.info("Task %s: killed %d job, failed to kill %s jobs." %(kwargs['task'], len(kwargs['task']['kill_ids'])-notkilled, notkilled))
        except Exception, exc:
            self.logger.error(str(traceback.format_exc()))
        finally:
            if kwargs['task']['kill_all']:
                setStatusTask(kwargs['task']['tm_taskname'], 'KILLED')
            else:
                setStatusTask(kwargs['task']['tm_taskname'], 'SUBMITTED')
        return Result(task=kwargs['task'], result=killed)

if __name__ == '__main__':
    import logging
    loglevel = logging.DEBUG
    logging.basicConfig(level=loglevel)
    logger = logging.getLogger('test kill')
    logger.debug("Logging level initialized to %s." %loglevel)
    ## here I show while I love Python's duck typing
    import collections
    task = {'tm_taskname': 'mattia001',
            'tm_user_dn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=mcinquil/CN=660800/CN=Mattia Cinquilli',
            'tm_user_vo': 'cms',
            'tm_user_group': '',
            'tm_user_role': '',
            'kill_ids': [1796224134],
            'kill_all': False}
    Sites = collections.namedtuple('Sites', 'available')
    Config = collections.namedtuple('Config', 'Sites')
    sites = Sites(available=['T2_CH_CERN'])
    cfg = Config(Sites=sites)
    pk = PanDAKill(cfg)
    result = pk.execute(task=task)
    print result
