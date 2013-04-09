#!/bin/bash

#touch Report.pkl

# should be a bit nicer than before
echo "Starting transformation..."

### source the CMSSW stuff using either OSG or LCG style entry env. variables
###    (incantations per oli's instructions)
#   LCG style --
if [ "x" != "x$VO_CMS_SW_DIR" ]
then
	. $VO_CMS_SW_DIR/cmsset_default.sh

#   OSG style --
elif [ "x" != "x$OSG_APP" ]
then
	. $OSG_APP/cmssoft/cms/cmsset_default.sh CMSSW_3_3_2
elif [ -e /cvmfs/cms.cern.ch ]
then
	export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
	. $VO_CMS_SW_DIR/cmsset_default.sh
else
	echo "Error: neither OSG_APP nor VO_CMS_SW_DIR environment variables were set" >&2
	echo "Error: Because of this, we can't load CMSSW. Not good." >&2
	exit 2
fi
echo "I think I found the correct CMSSW setup script"

if [ -e $VO_CMS_SW_DIR/COMP/$SCRAM_ARCH/external/python/2.6.4/etc/profile.d/init.sh ]
then
	. $VO_CMS_SW_DIR/COMP/$SCRAM_ARCH/external/python/2.6.4/etc/profile.d/init.sh
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$VO_CMS_SW_DIR/COMP/$SCRAM_ARCH/external/openssl/0.9.7m/lib:$VO_CMS_SW_DIR/COMP/$SCRAM_ARCH/external/bz2lib/1.0.5/lib
elif [ -e $OSG_APP/cmssoft/cms/COMP/$SCRAM_ARCH/external/python/2.6.4/etc/profile.d/init.sh ]
then
	. $OSG_APP/cmssoft/cms/COMP/$SCRAM_ARCH/external/python/2.6.4/etc/profile.d/init.sh
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$OSG_APP/cmssoft/cms/COMP/$SCRAM_ARCH/external/openssl/0.9.7m/lib:$OSG_APP/cmssoft/cms/COMP/$SCRAM_ARCH/external/bz2lib/1.0.5/lib
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

#get the transformation and unzip it
#wget http://common-analysis-framework.cern.ch/CMSRunAnaly.zip
#unzip CMSRunAnaly.zip

wget http://common-analysis-framework.cern.ch/CMSRunAnaly.tgz
tar xvfzm CMSRunAnaly.tgz

export PYTHONPATH=`pwd`/WMCore.zip:$PYTHONPATH
echo "Now running the job in `pwd`..."
python2.6 CMSRunAnaly.py -r "`pwd`" "$@"
jobrc=$?
echo "The job had an exit code of $jobrc "
exit $jobrc

_
