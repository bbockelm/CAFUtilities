from CAFUtilities.TaskDB.Interface.Task.SetTasks import setInjectedTasks, setFailedTasks
from CAFUtilities.TaskDB.Interface.JobGroup.MakeJobGroups import addJobGroup
import PandaServerInterface ## change this to specific import and add CAFUtilities
from CAFUtilities.taskbuffer.JobSpec import JobSpec
from CAFUtilities.taskbuffer.FileSpec import FileSpec

from TaskWorker.Actions.PanDAAction import PanDAAction
from TaskWorker.DataObjects.Result import Result

import commands
import urllib
import time


class PanDAInjection(PanDAAction):
    """Creating the specs and injecting them into PanDA"""

    def inject(self, task, pandajobspecs):
        pandajobspecs = pandajobspecs[0:3]
        status, tmpOut = PandaServerInterface.submitJobs(pandajobspecs, task['tm_user_dn'], task['tm_user_vo'], task['tm_user_group'], task['tm_user_role'], True)
        self.logger.info('jobgroup %s - panda submission status: %s' % (tmpOut, status))
        jobDefinitionID = None
        try:
            jobDefinitionID = int(tmpOut[0][1])
            self.logger.debug('Panda submission out has resulted in job definition ID = %s' % str(jobDefinitionID))
        except Exception, ex:
            self.logger.error('Problem injecting into PanDa ' + str(ex))
            ## TODO: handle this failure
            pass
        return 333333, jobDefinitionID

    def makeSpecs(self, task, jobgroup, site, jobset):
        PandaServerInterface.refreshSpecs()
        pandajobspec = []
        for job in jobgroup.jobs:
            pandajobspec.append(self.createJobSpec(task, job, jobset, site))
        return pandajobspec

    def createJobSpec(self, task, job, jobset, site):
        #jobdef, jobsetID, user, userDN, outdsname, userisb, cacheurl, swver, idx, computingsite, asyncdest
        datasetname = 'user.%s.%s' % (task['tm_username'], task['tm_publish_name'])
        
        pandajob = JobSpec()
        #job.jobDefinitionID   = 1 #jobDefinitionI
        #job.jobsetID = int(time.time()) % 10000
        if jobset:
            pandajob.jobsetID = jobset
        pandajob.jobName = "%s" % commands.getoutput('uuidgen')
        #print 'pandajob.jobName = ', pandajob.jobName
        pandajob.prodUserID = task['tm_user_dn']
        #print 'pandajob.prodUserID = ', pandajob.prodUserID
        pandajob.destinationDBlock = datasetname
        #print 'pandajob.destinationDBlock = ', pandajob.destinationDBlock
        pandajob.prodSourceLabel = 'user'
        #print 'pandajob.prodSourceLabel = ', pandajob.prodSourceLabel
        pandajob.computingSite = site
        #print 'pandajob.computingSite = ', pandajob.computingSite
        pandajob.cloud = PandaServerInterface.PandaSites[pandajob.computingSite]['cloud']
        #print 'pandajob.cloud = ', pandajob.cloud
        pandajob.destinationSE = 'local'
        #print 'pandajob.destinationSE = ', pandajob.destinationSE
        pandajob.transformation = '%s/%s' % (PandaServerInterface.baseURLSUB, task['tm_transformation'])
        #print 'pandajob.transformation = ', pandajob.transformation
        pandajob.jobParameters = '-j "" '

        infilestring = ''
        for inputfile in job['input_files']:
            infilestring += '%s,' % inputfile['lfn']
        infilestring = infilestring[:-1]

        execstring = "CMSSW.sh %s %d %s '%s' %s" % (pandajob.jobName, 1, task['tm_job_sw'], infilestring, task['tm_user_sandbox'])

        pandajob.jobParameters += '-p "%s" ' % urllib.quote(execstring)
        pandajob.jobParameters += '--sourceURL %s ' % task['tm_cache_url']
        pandajob.jobParameters += '-a %s ' % task['tm_user_sandbox']
        pandajob.jobParameters += '-r . '
        pandajob.jobParameters += '-o "{%s: %s}" ' % ("'outfile.root'", "'outfilehassen.root'")
        pandajob.jobParameters += 'parametroTest'
        #print 'pandajob.jobParameters = ', pandajob.jobParameters

        logfile = FileSpec()
        logfile.lfn = "%s.job.log.tgz" % pandajob.jobName
        #print 'logfile.lfn = ', logfile.lfn
        logfile.destinationDBlock = pandajob.destinationDBlock
        #print 'logfile.destinationDBlock = ', logfile.destinationDBlock
        logfile.destinationSE = task['tm_asyncdest']
        #print 'logfile.destinationSE = ', logfile.destinationSE
        logfile.dataset = pandajob.destinationDBlock
        #print 'logfile.dataset = ', logfile.dataset
        logfile.type = 'log'
        #print 'logfile.type = ', logfile.type
        pandajob.addFile(logfile)

        outfile = FileSpec()
        outfile.lfn = "outfilehassen.root" 
        #print 'outfile.lfn = ', outfile.lfn
        outfile.destinationDBlock = pandajob.destinationDBlock
        #print 'outfile.destinationDBlock = ', outfile.destinationDBlock
        outfile.destinationSE = task['tm_asyncdest']
        #print 'outfile.destinationSE = ', outfile.destinationSE
        outfile.dataset = pandajob.destinationDBlock
        #print 'outfile.dataset = ', outfile.dataset
        outfile.type = 'output'
        #print 'outfile.type = ', outfile.type
        pandajob.addFile(outfile)

        #print pandajob
        #raise Exception('aaa')
        return job

    def execute(self, *args, **kwargs):
        self.logger.info(" create specs and inject into PanDA ")
        results = []
        jobset = None
        for jobgroup in args[0]:
            jobs, site = jobgroup.result
            #try:
            #    taskName, jobdefid, status, blocks, jobgroup_failure
            jobset, jobdef = self.inject(kwargs['task'], self.makeSpecs(kwargs['task'], jobs, site, jobset))
            #except Exception, exc:    
            #    #print exc
            #    addJobGroup(kwargs['task']['tm_taskname'], "Failed", "blocks-1", str(exc))
            #else:
            #    pass#addJobGroup(kwargs['task']['tm_taskname'], jobdef, "submitted", "blocks-1", None)
        if jobset:
            setInjectedTasks(kwargs['task']['tm_taskname'], "Submitted", jobset)
        else:
            setFailedTasks(kwargs['task']['tm_taskname'], "Failed", "all problems here")
        return results
