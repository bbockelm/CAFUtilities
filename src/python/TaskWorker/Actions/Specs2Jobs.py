from WMCore.DataStructs.JobGroup import JobGroup as WMJobGroup
from WMCore.DataStructs.Job import Job as WMJob

import PandaServerInterface ## change this to specific imports

from TaskWorker.Actions.TaskAction import TaskAction
from TaskWorker.DataObjects.Result import Result

#from urllib import unquote
from ast import literal_eval


class Specs2Jobs(TaskAction):
    """Given a list of job specs to be resubmitted, transforms the specs
       into jobgroups-jobs structure in order to reflect the splitting output."""

    def execute(self, *args, **kwargs):
        self.logger.info("Transforming old specs into jobs.")
        # need to remake the job groups and group the jobs by jobgroups
        # depending on the data the jobs need to access

        locationsjobs = {}
        ## grouping in a dictionary can happen here
        for job in args[0]:
            if job.computingSite in locationsjobs:
                locationsjobs[job.computingSite].append(job)
            else:
                locationsjobs[job.computingSite] = [job]

        jobgroups = []
        ## here converting the grouping into proper JobGroup-Jobs
        for site in locationsjobs:
            jg = WMJobGroup()
            for job in locationsjobs[site]:
                # this is soooo ugly 
                inputfiles = literal_eval(literal_eval(job.jobParameters.split('--inputFile=')[-1].split('--lumiMask')[0]))
                jj = WMJob()
                jj['input_files'] = []
                for infile in inputfiles:
                    jj['input_files'].append({'lfn': infile, 'block': 'unknown', 'locations': [site.split('ANALY_')[-1]]})
                # this is soooo ugly 
                jj['mask']['runAndLumis'] = literal_eval(job.jobParameters.split('--lumiMask=')[-1].split(' -o')[0])
                jj['panda_oldjobid'] = job.PandaID
                jg.add(jj)
            jg.commit()
            jobgroups.append(jg)

        return Result(task=kwargs['task'], result=jobgroups)
