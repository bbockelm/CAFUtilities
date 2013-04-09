
import os
import json

dag_fragment = """
JOB Job%(count)d Job.submit
#SCRIPT PRE  Job%(count)d dag_bootstrap.sh PREJOB $RETRY $JOB
#SCRIPT POST Job%(count)d dag_bootstrap.sh POSTJOB $RETRY $JOB
#PRE_SKIP Job%(count)d 3
RETRY Job%(count)d 3
VARS Job%(count)d count="%(count)d" runAndLumiMask="%(runAndLumiMask)s" inputFiles="%(inputFiles)s" +desiredSites="%(desiredSites)s"

JOB ASO%(count)d ASO.submit
VARS ASO%(count)d count="%(count)d" outputFiles="%(outputFiles)s"

PARENT Job%(count)d CHILD ASO%(count)d
"""

def make_specs(self, jobgroup, availablesites, outfiles, startjobid):
    specs = []
    i = startjobid
    for job in jobgroup.getJobs():
        inputFiles = json.dumps([inputfile['lfn'] for inputfile in job['input_files']]).replace('"', r'\"\"')
        runAndLumiMask = json.dumps(job['mask']['runAndLumis']).replace('"', r'\"\"')
        desiredSites = ", ".join(availablesites)
        i += 1
        specs.append({'count': i, 'runAndLumiMask': runAndLumiMask, 'inputFiles': inputFiles,
                      'desiredSites': desiredSites, 'outputFiles': outfiles})
        print specs[-1]
    return specs, i

def create_subdag(splitter_result, **kwargs):

    startjobid = 0
    specs = []

    outfiles = kwargs['task']['tm_outfiles'] + kwargs['task']['tm_tfile_outfiles'] + kwargs['task']['tm_edm_outfiles']

    os.chmod("CMSRunAnaly.sh", 0755)

    #fixedsites = set(self.config.Sites.available)
    for jobgroup in splitter_result:
        jobs = jobgroup.getJobs()
        if not jobs:
            possiblesites = []
        else:
            possiblesites = jobs[0]['input_files'][0]['locations']
        availablesites = set(kwargs['task']['tm_site_whitelist']) if kwargs['task']['tm_site_whitelist'] else set(possiblesites) & \
                         set(possiblesites) - \
                         set(kwargs['task']['tm_site_blacklist'])
        #availablesites = set(availablesites) & fixedsites
        availablesites = list(availablesites)

        if not availablesites:
            msg = "No site available for submission of task %s" % (kwargs['task'])
            raise NoAvailableSite(msg)

        jobgroupspecs, startjobid = make_specs(kwargs['task'], jobgroup, availablesites, outfiles, startjobid)
        specs += jobgroupspecs

    dag = ""
    for spec in specs:
        dag += dag_fragment % spec

    with open("RunJobs.dag", "w") as fd:
        fd.write(dag)

def async_stageout():
    raise NotImplementedError()
    return

def postjob(retry_str, job):
    retry = int(retry_str)
    raise NotImplementedError()
    return 0

def prejob():
    retry = int(retry_str)
    raise NotImplementedError()
    return 0

