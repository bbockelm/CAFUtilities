from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result

class DataDiscovery(TaskAction):
    """I am the abstract class for the data discovery.
       Taking care of generalizing different data discovery
       possibilities. Implementing only a common method to
       return a properly formatted output."""

    def formatOutput(self, *args, **kwargs):
        """Receives as input the result of the data location
           discovery operations and fill up the WMCore objects."""
        self.logger.info(" Formatting data discovery output ")
        return Result()
