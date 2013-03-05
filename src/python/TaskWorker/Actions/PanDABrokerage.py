from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result

class PanDABrokerage(PanDAAction):
    """Given a list of possible sites, ask PanDA which one is the
       best one at the current time for the job submission."""

    def execute(self, *args, **kwargs):
        self.logger.info(" asking best site to PanDA ")
        return Result()
