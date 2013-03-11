from CAFUtilities.TaskDB.Interface.Task.SetTasks import setInjectedTasks, setFailedTasks
from CAFUtilities.TaskDB.Interface.JobGroup.MakeJobGroups import addJobGroup

from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result

class PanDAInjection(PanDAAction):
    """Creating the specs and injecting them into PanDA"""

    def inject(self, specs):
        pass

    def makeSpecs(self, jobgroup):
        pass

    def execute(self, *args, **kwargs):
        self.logger.info(" create specs and inject into PanDA ")
        results = []
        #print args
        #print kwargs
        for jobgroup in args[0]:
            #print jobgroup.result
            jobs, site = jobgroup.result
            try:
                self.inject(self.makeSpecs(jobs, site))
            except Exception, exc:    
                addJobGroup(kwargs['task']['tm_taskname'], "Failed", "blocks-1", str(exc))
            else:
                addJobGroup(kwargs['task']['tm_taskname'], 1004, "submitted", "blocks-1", None)
        setInjectedTasks(kwargs['task']['tm_taskname'], "submitted", 1004)
        #setFailedTasks("hassen_tasks_106", "Failed", "DM problem")
        return results
