
import os
import sys
import errno
import pickle

import classad

import TaskWorker.Actions.DBSDataDiscovery as DBSDataDiscovery
import TaskWorker.Actions.Splitter as Splitter
import TaskWorker.Actions.DagmanCreator as DagmanCreator

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

    ad['tm_taskname'] = classad.ExprTree("CRAB.Workflow")
    ad['tm_split_algo'] = classad.ExprTree("CRAB.SplitAlgo")
    ad['tm_split_args'] = classad.ExprTree("CRAB.AlgoArgs")
    ad['tm_dbs_url'] = classad.ExprTree("CRAB.DBSUrl")
    ad['tm_input_dataset'] = classad.ExprTree("CRAB.InputData")

    if command == "DBS":
        task = DBSDataDiscovery(*in_args)
    elif command == "SPLIT":
        task = Splitter.Splitter()
    results = task.execute(*in_args, task=ad)
    if command == "SPLIT":
        results = DagmanCreator.create_subdag(results)

    with open(outfile, "r") as fd:
        pickle.dump(fd, results)

    return 0

if __name__ == '__main__':
    sys.exit(bootstrap())

