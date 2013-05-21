#! /bin/sh

set -x

BASEDIR=$(cd "$(dirname "$0")"; pwd)

ORIGDIR=$PWD
STARTDIR=$PWD/tmp/runtime

WMCOREDIR=$STARTDIR/WMCore
WMCOREVER=0.9.59-dagman3
WMCOREREPO=bbockelm

TASKWORKERDIR=$STARTDIR/TaskWorker
TASKWORKERVER=0.1.2-dagman5
TASKWORKERREPO=bbockelm

CAFUTILITIESDIR=$STARTDIR/CAFUtilities
CAFUTILITIESVER=0.1-dagman3
CAFUTILITIESREPO=bbockelm

DBSDIR=$STARTDIR/DBS
DBSVER=DBS_2_1_9-dagman2
DBSREPO=bbockelm

DLSDIR=$STARTDIR/DLS
DLSVER=DLS_1_1_3
DLSREPO=bbockelm

CRABSERVERDIR=$STARTDIR/CRABServer
CRABSERVERVER=3.1.0-dagman7
CRABSERVERREPO=bbockelm

CRABCLIENTDIR=$STARTDIR/CRABClient
CRABCLIENTVER=3.1.1-dagman5
CRABCLIENTREPO=bbockelm

rm -rf $STARTDIR

mkdir -p $WMCOREDIR
mkdir -p $TASKWORKERDIR
mkdir -p $DBSDIR
mkdir -p $DLSDIR
mkdir -p $CRABSERVERDIR
mkdir -p $CRABCLIENTDIR

cp $BASEDIR/gWMS-CMSRunAnaly.sh $STARTDIR || exit 3

pushd $STARTDIR

curl -L https://github.com/$CAFUTILITIESREPO/CAFUtilities/archive/$CAFUTILITIESVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$WMCOREREPO/WMCore/archive/$WMCOREVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$TASKWORKERREPO/CAFTaskWorker/archive/$TASKWORKERVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$DBSREPO/DBS/archive/$DBSVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$DLSREPO/DLS/archive/$DLSVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$CRABSERVERREPO/CRABServer/archive/$CRABSERVERVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$CRABCLIENTREPO/CRABClient/archive/$CRABCLIENTVER.tar.gz | tar zx || exit 2
curl -L https://httplib2.googlecode.com/files/httplib2-0.8.tar.gz | tar zx || exit 2
curl -L http://download.cherrypy.org/cherrypy/3.2.2/CherryPy-3.2.2.tar.gz | tar zx || exit 2
curl -L https://pypi.python.org/packages/source/S/SQLAlchemy/SQLAlchemy-0.8.0.tar.gz | tar zx || exit 2
curl -L http://hcc-briantest.unl.edu/CRAB3-condor-libs.tar.gz | tar zx *.so* || exit 2
curl -L http://cmsrep.cern.ch/cmssw/cms/RPMS/slc5_amd64_gcc462/external+py2-pyopenssl+0.11-1-1.slc5_amd64_gcc462.rpm | rpm2cpio | cpio -uimd || exit 2

pushd WMCore-$WMCOREVER/src/python
zip -r $STARTDIR/CRAB3.zip WMCore PSetTweaks || exit 3
popd

pushd CAFTaskWorker-$TASKWORKERVER/src/python
zip -r $STARTDIR/CRAB3.zip TaskWorker || exit 3
popd

pushd CAFUtilities-$CAFUTILITIESVER/src/python
zip -r $STARTDIR/CRAB3.zip TaskDB || exit 3
popd

pushd DBS-$DBSVER/Clients/Python
zip -r $STARTDIR/CRAB3.zip DBSAPI || exit 3
popd

pushd DLS-$DLSVER/Client/LFCClient
zip -r $STARTDIR/CRAB3.zip *.py || exit 3
popd

pushd CRABClient-$CRABCLIENTVER/src/python
zip -r $STARTDIR/CRAB3.zip CRABClient || exit 3
cp ../../bin/crab $STARTDIR/
cp ../../bin/crab3 $STARTDIR/
popd

pushd CRABServer-$CRABSERVERVER/src/python
zip -r $STARTDIR/CRAB3.zip CRABInterface || exit 3
popd

pushd httplib2-0.8/python2
zip -r $STARTDIR/CRAB3.zip httplib2 || exit 3
popd

pushd CherryPy-3.2.2/
zip -r $STARTDIR/CRAB3.zip cherrypy || exit 3
popd

pushd opt/cmssw/slc5_amd64_gcc462/external/py2-pyopenssl/0.11/lib/python2.6/site-packages
mv OpenSSL $STARTDIR/lib/python/

cat > setup.sh << EOF
export CRAB3_BASEPATH=\`dirname \${BASH_SOURCE[0]}\`
export PATH=\$CRAB3_BASEPATH:\$PATH
export PYTHONPATH=\$CRAB3_BASEPATH/CRAB3.zip:\$CRAB3_BASEPATH/lib/python:\$PYTHONPATH
export LD_LIBRARY_PATH=\$CRAB3_BASEPATH/lib:\$CRAB3_BASEPATH/lib/condor:\$LD_LIBRARY_PATH
EOF

mkdir -p bin
cp CRABServer-$CRABSERVERVER/bin/* bin/
cp CAFUtilities-$CAFUTILITIESVER/src/python/transformation/CMSRunAnaly.sh bin/

tar zcf $ORIGDIR/TaskManagerRun.tar.gz CRAB3.zip setup.sh crab3 crab gWMS-CMSRunAnaly.sh bin || exit 4
tar zcf $ORIGDIR/CRAB3-gWMS.tar.gz CRAB3.zip setup.sh crab3 crab gWMS-CMSRunAnaly.sh bin lib || exit 4

popd

