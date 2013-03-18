#! /bin/sh

set -x

DEPLOYVER=12.10c
DEPLOYCFG=deployment-$DEPLOYVER
STARTDIR=$PWD
WORKDIR=TaskWorker
SOURCEDIR=$STARTDIR/$WORKDIR/source
LOGDIR=$WORKDIR/logs/
LOGFILE=$LOGDIR/TaskWorker.log
ROTATECONF=rotatesplitter

mkdir -p $LOGDIR
mkdir -p $SOURCEDIR
touch $LOGFILE

cd $WORKDIR

wget --no-check-certificate https://github.com/dmwm/deployment/archive/$DEPLOYVER.tar.gz
tar -xvzf $DEPLOYVER

cd $DEPLOYCFG
./Deploy -r comp=comp -R wmagent@0.9.25 -s prep -A slc5_amd64_gcc461 -t v01 $STARTDIR/$WORKDIR   wmagent
./Deploy -r comp=comp -R wmagent@0.9.25 -s sw -A slc5_amd64_gcc461 -t v01 $STARTDIR/$WORKDIR   wmagent
./Deploy -r comp=comp -R wmagent@0.9.25 -s post -A slc5_amd64_gcc461 -t v01 $STARTDIR/$WORKDIR   wmagent

cd $STARTDIR
cd $SOURCEDIR
wget --no-check-certificate https://github.com/dmwm/WMCore/archive/0.9.25.tar.gz
tar -xvzf 0.9.25
mv WMCore-0.9.25 WMCore
wget --no-check-certificate https://github.com/mmascher/WMCore/commit/56a410bdec25bee5ebd53016c13beb30aa9bad8d.patch
git apply -p1 -C3 --directory=WMCore 56a410bdec25bee5ebd53016c13beb30aa9bad8d.patch

OLDSSH_ASKPASS=$SSH_ASKPASS
unset SSH_ASKPASS
git clone https://git.cern.ch/reps/CAFTaskWorker
git clone https://git.cern.ch/reps/CAFUtilities
set SSH_ASKPASS=$OLDSSH_ASKPASS
unset $OLDSSH_ASKPASS

set +x

source /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh && \
voms-proxy-init --voms cms --valid 190:0 && \
mkdir $STARTDIR/$WORKDIR/auth/ && \
scp /tmp/x509up_u$UID $STARTDIR/$WORKDIR/auth/proxy

cd $STARTDIR

# now generating launch-script
echo "#! /bin/sh
export STARTDIR=$STARTDIR
export WORKDIR=$WORKDIR
export SOURCEDIR=$SOURCEDIR
export X509_USER_PROXY=$STARTDIR/$WORKDIR/auth/proxy
source $STARTDIR/$WORKDIR/current/apps/wmagent/etc/profile.d/dependencies-setup.sh
export PYTHONPATH=$SOURCEDIR/CAFTaskWorker/src/python/:$SOURCEDIR/CAFUtilities/src/python/:$SOURCEDIR/WMCore/src/python/:\$PYTHONPATH" > $STARTDIR/env.sh
echo ""
echo "Now you need to source $STARTDIR/env.sh and execute"
echo " python $SOURCEDIR/CAFTaskWorker/src/python/TaskWorker/MasterWorker.py --config $SOURCEDIR/CAFTaskWorker/etc/TaskWorkerConfig.py --db-config YOUR_DB_CONFIG --debug"
