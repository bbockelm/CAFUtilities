from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result

class PanDAKill(PanDAAction):
    """Ask PanDA to kill jobs."""

    def execute(self, *args, **kwargs):
        self.logger.info(" killing injected jobs ")
        return Result()
