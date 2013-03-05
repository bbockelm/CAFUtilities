from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result

class PanDAInjection(PanDAAction):
    """Creating the specs and injecting them into PanDA"""

    def execute(self, *args, **kwargs):
        self.logger.info(" create specs and inject into PanDA ")
        return Result()
