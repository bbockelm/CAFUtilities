from TaskDB.Interface.Task.SetTasks import setInjectedTasks, setFailedTasks
from TaskDB.Interface.JobGroup.MakeJobGroups import addJobGroup
import PandaServerInterface ## change this to specific imports
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result
from TaskWorker.WorkerExceptions import PanDAIdException, PanDAException, NoAvailableSite

import time
import commands
import traceback
import os
import json
import string
import random


# the length of the string to be appended to the every out file (before their extension)
LFNHANGERLEN = 6


class PanDAInjection(PanDAAction):
    """Creating the specs and injecting them into PanDA"""

    def inject(self, task, pandajobspecs):
        """Injects in PanDA the taskbuffer objects containing the jobs specs.

           :arg TaskWorker.DataObject.Task task: the task to work on
           :arg list taskbuffer.JobSpecs pandajobspecs: the list of specs to inject
           :return: dictionary containining the injection resulting id's."""
        #pandajobspecs = pandajobspecs[0:2]
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

    def makeSpecs(self, task, outdataset, jobgroup, site, jobset, jobdef, startjobid, basejobname, lfnhanger):
        """Building the specs

        :arg TaskWorker.DataObject.Task task: the task to work on
        :arg str outdataset: the output dataset name where all the produced files will be placed
        :arg WMCore.DataStructs.JobGroup jobgroup: the group containing the jobs
        :arg str site: the borkered site where to run the jobs
        :arg int jobset: the PanDA jobset corresponding to the current task
        :arg int jobdef: the PanDA jobdef where to append the current jobs --- not used
        :arg int startjobid: jobs need to have an incremental index, using this to have
                             unique ids in the whole task
        :arg str basejobname: common string between all the jobs in their job name
        :return: the list of job sepcs objects."""
        PandaServerInterface.refreshSpecs()
        pandajobspec = []
        i = startjobid
        for job in jobgroup.jobs:
            #if i > 10:
            #    break
            jobname = "%s-%d" %(basejobname, i)
            pandajobspec.append(self.createJobSpec(task, outdataset, job, jobset, jobdef, site, jobname, lfnhanger, i))
            i += 1
        return pandajobspec, i

    def createJobSpec(self, task, outdataset, job, jobset, jobdef, site, jobname, lfnhanger, jobid):
        """Create a spec for one job

        :arg TaskWorker.DataObject.Task task: the task to work on
        :arg str outdataset: the output dataset name where all the produced files will be placed
        :arg WMCore.DataStructs.Job job: the abstract job
        :arg int jobset: the PanDA jobset corresponding to the current task
        :arg int jobdef: the PanDA jobdef where to append the current jobs --- not used
        :arg str site: the borkered site where to run the jobs
        :arg str jobname: the job name
        :arg str lfnhanger: the random string to be added in the output file name
        :arg int jobid: incremental job number
        :return: the sepc object."""

        pandajob = JobSpec()
        ## always setting a job definition ID
        pandajob.jobDefinitionID = jobdef if jobdef else -1
        ## always setting a job set ID
        pandajob.jobsetID = jobset if jobset else -1
        pandajob.jobName = jobname
        pandajob.prodUserID = task['tm_user_dn']
        pandajob.destinationDBlock = outdataset
        pandajob.prodSourceLabel = 'user'
        pandajob.computingSite = site
        pandajob.cloud = PandaServerInterface.PandaSites[pandajob.computingSite]['cloud']
        pandajob.destinationSE = 'local'
        pandajob.transformation = task['tm_transformation']
        ## need to initialize this
        pandajob.metadata = ''

        def outFileSpec(of=None, log=False):
            """Local routine to create an FileSpec for the an job output/log file

               :arg str of: output file base name
               :return: FileSpec object for the output file."""
            outfile = FileSpec()
            if log:
                outfile.lfn = "job.log_%d_%s.tgz" % (jobid, lfnhanger)
                outfile.type = 'log'
            else:
                outfile.lfn = '%s_%d_%s%s' %(os.path.splitext(of)[0], jobid, lfnhanger, os.path.splitext(of)[1])
                outfile.type = 'output'
            outfile.destinationDBlock = pandajob.destinationDBlock
            outfile.destinationSE = task['tm_asyncdest']
            outfile.dataset = pandajob.destinationDBlock
            return outfile

        alloutfiles = []
        outjobpar = {}
        outfilestring = ''
        for outputfile in task['tm_outfiles']:
            outfilestring += '%s,' % outputfile
            filespec = outFileSpec(outputfile)
            alloutfiles.append(filespec)
            #pandajob.addFile(filespec)
            outjobpar[outputfile] = filespec.lfn
        for outputfile in task['tm_tfile_outfiles']:
            outfilestring += '%s,' % outputfile
            filespec = outFileSpec(outfputile)
            alloutfiles.append(filespec)
            #pandajob.addFile(filespec)
            outjobpar[outputfile] = filespec.lfn
        for outputfile in task['tm_edm_outfiles']:
            outfilestring += '%s,' % outputfile
            filespec = outFileSpec(outputfile)
            alloutfiles.append(filespec)
            #pandajob.addFile(filespec)
            outjobpar[outputfile] = filespec.lfn
        outfilestring = outfilestring[:-1]

        infiles = []
        for inputfile in job['input_files']:
            infiles.append( inputfile['lfn'] )

        pandajob.jobParameters    = ''
        pandajob.jobParameters    += '-a %s ' % task['tm_user_sandbox']
        pandajob.jobParameters    += '--sourceURL %s ' % task['tm_cache_url']
        pandajob.jobParameters    += '--jobNumber=%s ' % jobid
        pandajob.jobParameters    += '--cmsswVersion=%s ' % task['tm_job_sw']
        pandajob.jobParameters    += '--scramArch=%s ' % task['tm_job_arch']
        pandajob.jobParameters    += '--inputFile=\'%s\' ' % json.dumps(infiles)
        pandajob.jobParameters    += '--lumiMask=\'%s\' ' % json.dumps(job['mask']['runAndLumis'])
        pandajob.jobParameters    += '-o "%s" ' % str(outjobpar)
        #job.jobParameters    += '%s ' % str(wfid) #TODO Is it necessary? Why has it been removed?

        if 'panda_oldjobid' in job and job['panda_oldjobid']:
            pandajob.parentID = job['panda_oldjobid']

        pandajob.addFile(outFileSpec(log=True))
        for filetoadd in alloutfiles:
            pandajob.addFile(filetoadd)

        return pandajob

    def execute(self, *args, **kwargs):
        self.logger.info(" create specs and inject into PanDA ")
        results = []
        ## in case the jobset already exists it means the new jobs need to be appended to the existing task
        ## (a possible case for this is the resubmission)
        jobset = kwargs['task']['panda_jobset_id'] if kwargs['task']['panda_jobset_id'] else None
        jobdef = None
        startjobid = 0

        basejobname = "%s" % commands.getoutput('uuidgen')
        lfnhanger = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(LFNHANGERLEN))
        ## /<primarydataset>/<yourHyperNewsusername>-<publish_data_name>-<PSETHASH>/USER
        outdataset = '/%s/%s-%s/USER' %(kwargs['task']['tm_input_dataset'].split('/')[1],
                                        kwargs['task']['tm_username'],
                                        kwargs['task']['tm_publish_name'])

        for jobgroup in args[0]:
            jobs, site = jobgroup.result
            blocks = [infile['block'] for infile in jobs.jobs[0]['input_files'] if infile['block']]
            # now try/except everything, then need to put the code in smaller try/except cages
            # note: there is already an outer try/except for every action and for the whole handler
            try:
                if not site:
                    msg = "No site available for submission of task %s" %(kwargs['task'])
                    raise NoAvailableSite(msg)

                jobgroupspecs, startjobid = self.makeSpecs(kwargs['task'], outdataset, jobs, site,
                                                           jobset, jobdef, startjobid, basejobname, lfnhanger)
                jobsetdef = self.inject(kwargs['task'], jobgroupspecs)
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
                results.append(Result(task=kwargs['task'], result=jobsetdef))
            except Exception, exc:
                msg = "Problem %s injecting job group from task %s reading data from blocks %s" % (str(exc), kwargs['task'], ",".join(blocks))
                self.logger.error(msg)
                self.logger.error(str(traceback.format_exc()))
                addJobGroup(kwargs['task']['tm_taskname'], None, "Failed", ",".join(blocks), str(exc))
                results.append(Result(task=kwargs['task'], warn=msg))
        if not jobset:
            msg = "No task id available for the task. Setting %s at failed." % kwargs['task']
            self.logger.error(msg)
            setFailedTasks(kwargs['task']['tm_taskname'], "Failed", msg)
            results.append(Result(task=kwargs['task'], err=msg))
        return results
