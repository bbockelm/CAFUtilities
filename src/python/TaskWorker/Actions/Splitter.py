from WMCore.DataStructs.Subscription import Subscription
from WMCore.DataStructs.Workflow import Workflow
from WMCore.JobSplitting.SplitterFactory import SplitterFactory

from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result


class Splitter(TaskAction):
    """Performing the split operation depending on the 
       recevied input and arguments"""

    def execute(self, *args, **kwargs):
        wmwork = Workflow(name=kwargs['task']['name'])
        wmsubs = Subscription(fileset=args[0], workflow=wmwork,
                              split_algo=kwargs['task']['splitalgo'], type="Processing")
        splitter = SplitterFactory()
        jobfactory = splitter(subscription=wmsubs)
        splitparam = kwargs['task']['splitargs']
        splitparam['algorithm'] = kwargs['task']['splitalgo']
        return Result(result=jobfactory(**splitparam))


if __name__ == '__main__':
    splitparams = [{'halt_job_on_file_boundaries': False, 'algorithm': 'LumiBased', 'lumis_per_job': 2000, 'splitOnRun': False},
                   {'halt_job_on_file_boundaries': False, 'algorithm': 'LumiBased', 'lumis_per_job': 50, 'splitOnRun': False},
                   {'algorithm': 'FileBased', 'files_per_job': 2000, 'splitOnRun': False},
                   {'algorithm': 'FileBased', 'files_per_job': 50, 'splitOnRun': False},]
