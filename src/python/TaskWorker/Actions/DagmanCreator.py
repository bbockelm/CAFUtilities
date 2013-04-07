
dag_fragment = """
JOB Job%{count}d Job.submit
SCRIPT PRE  Job%{count}d dag_bootstrap.sh PREJOB $RETRY $JOB
SCRIPT POST Job%{count}d dag_bootstrap.sh POSTJOB $RETRY $JOB
PRE_SKIP Job%{count}d 3
RETRY Job%{count}d 3
VARS Job%{count}d count="%{count}d" runAndLumiMask="%{runAndLumiMask}s" inputFiles="%{inputFiles}s" desiredSites="%{desiredSites}s"

JOB ASO%{count}d ASO.submit
VARS Job%{count}d count="%{count}d" outputFiles="%{outputFiles}s"

PARENT Job%{count}d ASO%{count}d
"""

def make_specs(self, jobgroup, availablesites, outfiles, startjobid):
     specs = []
     i = startjobid
     for job in jobgroup.jobs:
        inputFiles = json.dumps([inputfile['lfn'] for inputfile in job['input_files']])
        runAndLumiMask = json.dumps(job['mask']['runAndLumis'])
        desiredSites = json.dumps(availablesites)
        i += 1
        specs.append({'count': i, 'runAndLumiMask': runAndLumiMask, 'inputFiles': inputFiles,
                      'desiredSites': desiredSites, 'outputFiles': outfiles})
    return specs, i

def create_subdag(splitter_result, **kwargs):

    startjobid = 0
    specs = []

    outfiles = task['tm_outfiles'] + task['tm_tfile_outfiles'] + task['tm_edm_outfiles']

    fixedsites = set(self.config.Sites.available)
    for jobgroup in splitter_result:
        jobs = jobgroup.result
        if not jobs:
            possiblesites = []
        else:
            possiblesites = jobs[0]['input_files'][0]['locations']
        availablesites = set(kwargs['task']['tm_site_whitelist']) if kwargs['task']['tm_site_whitelist'] else set(possiblesites) &
                         set(possiblesites) -
                         set(kwargs['task']['tm_site_blacklist'])
        availablesites = list( set(availablesites) & fixedsites )

        if not availablesites:
            msg = "No site available for submission of task %s" % (kwargs['task'])
            raise NoAvailableSite(msg)

        jobgroupspecs, startjobid = self.makeSpecs(kwargs['task'], jobs, availablesites, outfiles, startjobid)
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

