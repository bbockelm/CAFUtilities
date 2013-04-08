
import os
import sys
import json
import errno
import types
import pickle

import classad

import TaskWorker.Actions.DBSDataDiscovery as DBSDataDiscovery
import TaskWorker.Actions.Splitter as Splitter
import TaskWorker.Actions.DagmanCreator as DagmanCreator

import WMCore.Configuration as Configuration

def bootstrap():

    command = sys.argv[1]
    if command == "PREJOB":
        return DagmanCreator.postjob(*sys.argv[2:])
    elif command == "POSTJOB":
        return DagmanCreator.prejob(*sys.argv[2:])
    elif command == "ASO":
        return async_stageout(*sys.argv[2:])

    infile, outfile = sys.argv[2:]

    adfile = os.environ["_CONDOR_JOB_AD"]

    with open(adfile, "r") as fd:
        ad = classad.parseOld(fd)

    in_args = []
    if infile != "None":
        with open(infile, "r") as fd:
            in_args = pickle.load(fd)

    config = Configuration.Configuration()
    config.section_("Services")
    config.Services.DBSUrl = 'http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'

    ad['tm_taskname'] = ad.eval("CRAB_Workflow")
    ad['tm_split_algo'] = ad.eval("CRAB_SplitAlgo")
    ad['tm_dbs_url'] = ad.eval("CRAB_DBSUrl")
    ad['tm_input_dataset'] = ad.eval("CRAB_InputData")
    ad['tm_outfiles'] = ad.eval("CRAB_AdditionalOutputFiles")
    ad['tm_tfile_outfiles'] = ad.eval("CRAB_TFileOutputFiles")
    ad['tm_edm_outfiles'] = ad.eval("CRAB_EDMOutputFiles")
    ad['tm_site_whitelist'] = ad.eval("CRAB_SiteWhitelist")
    ad['tm_site_blacklist'] = ad.eval("CRAB_SiteBlacklist")

    pure_ad = {}
    for key in ad:
        try:
            pure_ad[key] = ad.eval(key)
            if isinstance(pure_ad[key], classad.Value):
                del pure_ad[key]
            if isinstance(pure_ad[key], types.ListType):
                pure_ad[key] = [i.eval() for i in pure_ad[key]]
        except:
            pass
    ad = pure_ad
    ad['CRAB_AlgoArgs'] = json.loads(ad["CRAB_AlgoArgs"])
    ad['tm_split_args'] = ad["CRAB_AlgoArgs"]

    if command == "DBS":
        task = DBSDataDiscovery.DBSDataDiscovery(config)
    elif command == "SPLIT":
        task = Splitter.Splitter(config)
    results = task.execute(in_args, task=ad).result
    if command == "SPLIT":
        results = DagmanCreator.create_subdag(results, task=ad)

    print results
    with open(outfile, "w") as fd:
        pickle.dump(results, fd)

    return 0

if __name__ == '__main__':
    sys.exit(bootstrap())

