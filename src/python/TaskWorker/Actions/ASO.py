
#!/usr/bin/python

import os
import re
import sys
import time
import signal
import commands

import WMCore.Services.PhEDEx.PhEDEx as PhEDEx

fts_server = 'https://fts3-pilot.cern.ch:8443'

g_Job = None

def sighandler(*args):
    if g_Job:
        g_Job.cancel()

signal.signal(signal.SIGHUP,  sighandler)
signal.signal(signal.SIGINT,  sighandler)
signal.signal(signal.SIGTERM, sighandler)

REGEX_ID = re.compile("([a-f0-9]{8,8})-([a-f0-9]{4,4})-([a-f0-9]{4,4})-([a-f0-9]{4,4})-([a-f0-9]{12,12})")

class FTSJob(object):

    def __init__(self, transfer_list, count):
        self._id = None
        self._cancel = False
        self._sleep = 20
        self._transfer_list = transfer_list
        self._count = count
        with open("copyjobfile_%s" % count, "w") as fd:
            for source, dest in transfer_list:
                fd.write("%s %s\n" % (source, dest))

    def cancel(self):
        if self._id:
            cmd = "glite-transfer-cancel -s %s %s" % (fts_server, self._id)
            print "+", cmd
            os.system(cmd)

    def submit(self):
        cmd = "glite-transfer-submit -s %s -f copyjobfile_%s" % (fts_server, self._count)
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            raise Exception("glite-transfer-submit exited with status %s.\n%s" % (status, output))
        output = output.strip()
        print "Resulting transfer ID: %s" % output
        return output

    def status(self, long=False):
        if long:
            cmd = "glite-transfer-status -l -s %s %s" % (fts_server, self._id)
        else:
            cmd = "glite-transfer-status -s %s %s" % (fts_server, self._id)
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            raise Exception("glite-transfer-status exited with status %s.\n%s" % (status, output))
        return output.strip()

    def run(self):
        self._id = self.submit()
        if not REGEX_ID.match(self._id):
            raise Exception("Invalid ID returned from FTS transfer submit")
        idx = 0
        while True:
            idx += 1
            time.sleep(self._sleep)
            status = self.status()
            print status

            if status in ['Submitted', 'Pending', 'Ready', 'Active', 'Canceling', 'Hold']:
                continue

            #if status in ['Done', 'Finished', 'FinishedDirty', 'Failed', 'Canceled']:
            #TODO: Do parsing of "-l"
            if status in ['Done', 'Finished']:
                return 0

            if status in ['FinishedDirty', 'Failed', 'Canceled']:
                print self.status(True)
                return 1

def determineSizes(transfer_list):
    sizes = []
    for pfn in transfer_list:
        cmd = "lcg-ls -D srmv2 -b -l %s" % pfn
        print "+", cmd
        status, output = commands.getstatusoutput(cmd)
        if status:
            sizes.append(-1)
            continue
        info = output.split("\n")[0].split()
        if len(info) < 5:
            print "Invalid lcg-ls output:\n%s" % output
            sizes.append(-1)
            continue
        try:
            sizes.append(info[4])
        except ValueError:
            print "Invalid lcg-ls output:\n%s" % output
            sizes.append(-1)
            continue
    return sizes

def reportResults(job_id, dest_list, sizes):
    filtered_dest = [dest_list[i] for i in range(dest_list) if sizes[i] >= 0]
    filtered_sizes = [i for i in sizes if i >= 0]
    retval = 0

    cmd = 'condor_qedit %s OutputSizes "%s"' % (job_id, ",".join(filtered_sizes))
    print "+", cmd
    status, output = commands.getstatusoutput(cmd)
    if status:
        retval = status
        print output

    cmd = 'condor_qedit %s OutputPFNs "%s"' % (job_id, ",".join(filtered_dest))
    print "+", cmd
    status, output = commands.getstatusoutput(cmd)
    if status:
        retval = status
        print output

    return retval

def resolvePFNs(source_site, dest_site, source_dir, dest_dir, filenames):

    p = PhEDEx.PhEDEx()
    lfns = [os.path.join(source_dir, filename) for filename in filenames]
    lfns += [os.path.join(dest_dir, filename) for filename in filenames]
    dest_info = p.getPFN(nodes=[source_site, dest_site], lfns=lfns)

    results = []
    for filename in filenames:
        slfn = os.path.join(source_dir, filename)
        dlfn = os.path.join(dest_dir, filename)
        results.append((dest_info[source_site, slfn], dest_info[dest_site, dlfn]))
    return results

def async_stageout(dest_site, source_dir, dest_dir, count, job_id, *filenames, **kwargs):

    # Here's the magic.  Pull out the site the job ran at from its user log
    if 'source_site' not in kwargs:
        cmd = "condor_q -userlog job_log.%s -af MATCH_EXP_JOBGLIDEIN_CMSSite -af JOBGLIDEIN_CMSSite" % count
        status, output = commands.getstatusoutput(cmd)
        if status:
            print "Failed to query condor user log:\n%s" % output
            return 0
        match_site, source_site = output.split('\n')[0].split(" ", 1)
        # TODO: Testing mode.  If CMS site is not known, assume Nebraska
        if match_site == 'Unknown' or source_site == 'Unknown':
            source_site = 'T2_US_Nebraska'
    else:
        source_site = kwargs['source_site']

    transfer_list = resolvePFNs(source_site, dest_site, source_dir, dest_dir, filenames)
    for source, dest in transfer_list:
        print "Copying %s to %s" % (source, dest)

    global g_Job
    g_Job = FTSJob(transfer_list, count)
    fts_job_result = g_Job.run()

    source_list = [i[0] for i in transfer_list]
    dest_list = [i[1] for i in transfer_list]
    sizes = determineSizes(source_list)
    report_result = reportResults(job_id, dest_list, sizes)
    if report_result:
        return report_result

    failures = len([i for i in sizes if i<0])

    if failures:
        return failures

    return fts_job_result

if __name__ == '__main__':
    sys.exit(async_stageout("T2_US_Nebraska", '/store/user/bbockelm/crab_bbockelm_crab3_1', '/store/user/bbockelm', '1', 'dumper_16.root', 'dumper_17.root', source_site='T2_US_Nebraska'))

