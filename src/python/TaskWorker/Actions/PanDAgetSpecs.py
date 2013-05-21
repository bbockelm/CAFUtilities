import PandaServerInterface ## change this to specific imports

from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result


class PanDAgetSpecs(PanDAAction):
    """Given a list of jobs, this action retrieves the specs
       form PanDA and load them in memory."""

    def execute(self, *args, **kwargs):
        self.logger.info("Getting already existing specs ")
        status, pandaspecs = PandaServerInterface.getFullJobStatus(ids=kwargs['task']['resubmit_ids'],
                                                                   user=kwargs['task']['tm_user_dn'],
                                                                   vo=kwargs['task']['tm_user_vo'],
                                                                   group=kwargs['task']['tm_user_group'],
                                                                   role=kwargs['task']['tm_user_role'])
        return Result(task=kwargs['task'], result=pandaspecs)
