from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result

class PanDAgetSpecs(PanDAAction):
    """Given a list of jobs, this action retrieves the specs
       form PanDA and load them in memory."""

    def execute(self, *args, **kwargs):
        self.logger.info(" get already existing specs ")
        return Result()
