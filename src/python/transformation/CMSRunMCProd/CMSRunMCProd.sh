#!/bin/bash

#touch Report.pkl

# should be a bit nicer than before
echo "Starting transformation..."
TRF="CMSRunMCProd"
echo "$TRF"
 
### source the CMSSW stuff using either OSG or LCG style entry env. variables
###    (incantations per oli's instructions)
#   LCG style --
if [ "x" != "x$VO_CMS_SW_DIR" ]
then
        echo 'LCG style'  
	. $VO_CMS_SW_DIR/cmsset_default.sh
        declare -a VERSIONS
        VERSIONS=($(ls $VO_CMS_SW_DIR/$SCRAM_ARCH/external/python | grep 2.6))
        PY_PATH=$VO_CMS_SW_DIR/$SCRAM_ARCH/external/python
        echo 'python version: ' $VERSIONS
#   OSG style --
elif [ "x" != "x$OSG_APP" ]
then
        echo 'OSG style'  
	. $OSG_APP/cmssoft/cms/cmsset_default.sh CMSSW_3_3_2
        declare -a VERSIONS
        VERSIONS=($(ls $OSG_APP/cmssoft/cms/$SCRAM_ARCH/external/python | grep 2.6))
        PY_PATH=$OSG_APP/cmssoft/cms/$SCRAM_ARCH/external/python 
        echo 'python version: ' $VERSIONS
elif [ -e /cvmfs/cms.cern.ch ]
then
        export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
        . $VO_CMS_SW_DIR/cmsset_default.sh
else
	echo "Error: neither OSG_APP, CVMFS, nor VO_CMS_SW_DIR environment variables were set" >&2
	echo "Error: Because of this, we can't load CMSSW. Not good." >&2
	exit 2
fi
echo "I think I found the correct CMSSW setup script"

# check for Python2.6 installation 
N_PYTHON26=${#VERSIONS[*]}
if [ $N_PYTHON26 -lt 1 ]; then
    echo "ERROR: No Python2.6 installed in COMP area. It is broken!"
    exit 1
else
    VERSION=${VERSIONS[0]}
    echo "Python 2.6 found in $PY_PATH/$VERSION";
    PYTHON26=$PY_PATH/$VERSION
    # Initialize CMS Python 2.6
    source $PY_PATH/$VERSION/etc/profile.d/init.sh
fi

command -v python2.6 > /dev/null
rc=$?
if [[ $rc != 0 ]]
then
	echo "Error: Python2.6 isn't available on this worker node." >&2
	echo "Error: job execution REQUIRES python2.6" >&2
	exit
else
	echo "I found python2.6 at.."
	echo `which python2.6`
fi

wget http://common-analysis-framework.cern.ch/$TRF.tgz
tar xvfzm $TRF.tgz

export PYTHONPATH=`pwd`/WMCore.zip:$PYTHONPATH
echo "Now running the job in `pwd`..."
python2.6 $TRF.py -r "`pwd`" "$@"
jobrc=$?
echo "The job had an exit code of $jobrc "
exit $jobrc

_
