from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result

class Splitter(TaskAction):
    """Performing the split operation depending on the 
       recevied input and arguments"""

    def execute(self, *args, **kwargs):
        self.logger.info(" split the jobs ")
        return Result()
