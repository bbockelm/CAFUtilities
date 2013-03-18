from TaskDB.Interface.Task.SetTasks import setInjectedTasks, setFailedTasks
from TaskDB.Interface.JobGroup.MakeJobGroups import addJobGroup
import PandaServerInterface ## change this to specific imports
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result
from TaskWorker.WorkerExceptions import PanDAIdException, PanDAException

import time
import urllib2
import commands


class PanDAInjection(PanDAAction):
    """Creating the specs and injecting them into PanDA"""

    def inject(self, task, pandajobspecs):
        pandajobspecs = pandajobspecs[0:3]
        status, injout = PandaServerInterface.submitJobs(pandajobspecs, task['tm_user_dn'], task['tm_user_vo'], task['tm_user_group'], task['tm_user_role'], True)
        self.logger.info('PanDA submission exit code: %s' % status)
        jobsetdef = {}
        for jobid, defid, setid in injout:
            if setid['jobsetID'] in jobsetdef:
                jobsetdef[setid['jobsetID']].add(defid)
            else:
                jobsetdef[setid['jobsetID']] = set([defid])
        self.logger.debug('Single PanDA injection resulted in %d distinct jobsets and %d jobdefs.' % (len(jobsetdef), sum([len(jobsetdef[k]) for k in jobsetdef])))
        return jobsetdef

    def makeSpecs(self, task, jobgroup, site, jobset, jobdef):
        PandaServerInterface.refreshSpecs()
        pandajobspec = []
        for job in jobgroup.jobs:
            pandajobspec.append(self.createJobSpec(task, job, jobset, jobdef, site))
        return pandajobspec

    def createJobSpec(self, task, job, jobset, jobdef, site):
        datasetname = 'user/%s/%s' % (task['tm_username'], task['tm_publish_name'])
        
        pandajob = JobSpec()
        ## always setting a job definition ID
        pandajob.jobDefinitionID = jobdef if jobdef else 1
        ## always setting a job set ID
        pandajob.jobsetID = jobset if jobset else int(time.time()) % 10000 
        pandajob.jobName = "%s" % commands.getoutput('uuidgen')
        pandajob.prodUserID = task['tm_user_dn']
        pandajob.destinationDBlock = datasetname
        pandajob.prodSourceLabel = 'user'
        pandajob.computingSite = site
        pandajob.cloud = PandaServerInterface.PandaSites[pandajob.computingSite]['cloud']
        pandajob.destinationSE = 'local'
        pandajob.transformation = '%s/%s' % (PandaServerInterface.baseURLSUB, task['tm_transformation'])
        pandajob.jobParameters = '-j "" '

        infilestring = ''
        for inputfile in job['input_files']:
            infilestring += '%s,' % inputfile['lfn']
        infilestring = infilestring[:-1]

        execstring = "CMSSW.sh %s %d %s '%s' %s" % (pandajob.jobName, 1, task['tm_job_sw'], infilestring, task['tm_user_sandbox'])

        pandajob.jobParameters += '-p "%s" ' % urllib2.quote(execstring)
        pandajob.jobParameters += '--sourceURL %s ' % task['tm_cache_url']
        pandajob.jobParameters += '-a %s ' % task['tm_user_sandbox']
        pandajob.jobParameters += '-r . '
        pandajob.jobParameters += '-o "{%s: %s}" ' % ("'outfile.root'", "'outfilehassen.root'")
        pandajob.jobParameters += 'parametroTest'

        logfile = FileSpec()
        logfile.lfn = "%s.job.log.tgz" % pandajob.jobName
        logfile.destinationDBlock = pandajob.destinationDBlock
        logfile.destinationSE = task['tm_asyncdest']
        logfile.dataset = pandajob.destinationDBlock
        logfile.type = 'log'
        pandajob.addFile(logfile)

        outfile = FileSpec()
        outfile.lfn = "outfilehassen.root" 
        outfile.destinationDBlock = pandajob.destinationDBlock
        outfile.destinationSE = task['tm_asyncdest']
        outfile.dataset = pandajob.destinationDBlock
        outfile.type = 'output'
        pandajob.addFile(outfile)

        return pandajob

    def execute(self, *args, **kwargs):
        self.logger.info(" create specs and inject into PanDA ")
        results = []
        jobset = None
        jobdef = None
        for jobgroup in args[0]:
            jobs, site = jobgroup.result
            blocks = [infile['block'] for infile in jobs.jobs[0]['input_files'] if infile['block']]
            try:
                jobsetdef = self.inject(kwargs['task'], self.makeSpecs(kwargs['task'], jobs, site, jobset, jobdef))
                if len(jobsetdef) == 1:
                    outjobset = jobsetdef.keys()[0]
                    outjobdefs = jobsetdef[outjobset]

                    if outjobset is None:
                        msg = "Cannot retrieve the job set id for task %s " %kwargs['task']
                        raise PanDAException(msg)
                    elif jobset is None:
                        jobset = outjobset
                    elif not outjobset == jobset:
                        msg = "Task %s has jobgroups with different jobsets: %d and %d" %(kwargs['task'], outjobset, jobset)
                        raise PanDAIdException(msg)
                    else:
                        pass

                    for jd in outjobdefs:
                        addJobGroup(kwargs['task']['tm_taskname'], jd, "Submitted", ",".join(blocks), None)
                    setInjectedTasks(kwargs['task']['tm_taskname'], "Submitted", outjobset)
            except Exception, exc:
                msg = "Problem %s injecting job group from task %s reading data from blocks %s" % (str(exc), kwargs['task'], ",".join(blocks))
                self.logger.error(msg)
                addJobGroup(kwargs['task']['tm_taskname'], None, "Failed", ",".join(blocks), str(exc))
        if not jobset:
            
            setFailedTasks(kwargs['task']['tm_taskname'], "Failed", "all problems here")
        return results
