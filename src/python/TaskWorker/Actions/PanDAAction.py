from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result

class PanDAAction(TaskAction):
    """Generic PanDAAction. Probably not needed at the current stage
       but it since this should not cause a big overhead it would be 
       better to leave this here in order to eventually be ready to
       support specific PanDA interaction needs."""

    def __init__(self, pandaconfig):
        #super(TaskAction, self).__init__(config=pandaconfig)
        TaskAction.__init__(self, pandaconfig)
        ## TODO check some specific config? otherwise this can probably be removed  and default init could be used
        #print "specific panda action init", self
