
if [ "X$CRAB_DEV_BASE" = "X" ]; then

   export CRAB_DEV_BASE=~/projects

fi

export CRAB3_TEST_BASE=$CRAB_DEV_BASE/CAFTaskWorker

export PYTHONPATH=$CRAB_DEV_BASE/CAFTaskWorker/src/python:$CRAB_DEV_BASE/WMCore/src/python:$CRAB_DEV_BASE/CAFUtilities/src/python:$CRAB_DEV_BASE/DLS/Client/LFCClient:$CRAB_DEV_BASE/DBS/Clients/Python
export PYTHONPATH=$CRAB_DEV_BASE/CAFTaskWorker/test/python:$PYTHONPATH

exec pylint --rcfile=$CRAB_DEV_BASE/WMCore/standards/.pylintrc "$@"

