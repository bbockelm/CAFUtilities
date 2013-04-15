import PandaServerInterface ## change this to specific imports

from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result


class Specs2Jobs(TaskAction):
    """Given a list of job specs to be resubmitted, transforms the specs
       into jobgroups-jobs structure in order to reflect the splitting output."""

    def execute(self, *args, **kwargs):
        self.logger.info("Transforming old specs into jobs.")
        # need to remake the job groups and group the jobs by jobgroups
        # depending on the data the jobs need to access
        for job in args[0]:
            ## grouping in a dictionary can happen here
            pass

        jobgroups = []
        ## here converting the grouping into proper JobGroup-Jobs

        ## NOTE: at the resubmission we need to explicitely set the parentage,
        ##       preserve the job set id,
        ##       change the random string in the output LFNs,
        ##       plus run again the brokerage with PanDA.

        return Result(task=kwargs['task'], result=jobgroups)
