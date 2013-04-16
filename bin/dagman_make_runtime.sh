#! /bin/sh

set -x

ORIGDIR=$PWD
STARTDIR=$PWD/tmp/runtime

WMCOREDIR=$STARTDIR/WMCore
WMCOREVER=0.9.59-dagman
WMCOREREPO=bbockelm

TASKWORKERDIR=$STARTDIR/TaskWorker
TASKWORKERVER=0.1.1-dagman
TASKWORKERREPO=bbockelm

DBSDIR=$STARTDIR/DBS
DBSVER=DBS_2_1_9-dagman2
DBSREPO=bbockelm

DLSDIR=$STARTDIR/DLS
DLSVER=DLS_1_1_3
DLSREPO=bbockelm

CRABSERVERDIR=$STARTDIR/CRABServer
CRABSERVERVER=3.1.0-dagman
CRABSERVERREPO=bbockelm

CRABCLIENTDIR=$STARTDIR/CRABClient
CRABCLIENTVER=3.1.1-dagman2
CRABCLIENTREPO=bbockelm

rm -rf $STARTDIR

mkdir -p $WMCOREDIR
mkdir -p $TASKWORKERDIR
mkdir -p $DBSDIR
mkdir -p $DLSDIR
mkdir -p $CRABSERVERDIR
mkdir -p $CRABCLIENTDIR

pushd $STARTDIR

curl -L https://github.com/$WMCOREREPO/WMCore/archive/$WMCOREVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$TASKWORKERREPO/CAFTaskWorker/archive/$TASKWORKERVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$DBSREPO/DBS/archive/$DBSVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$DLSREPO/DLS/archive/$DLSVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$CRABSERVERREPO/CRABServer/archive/$CRABSERVERVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$CRABCLIENTREPO/CRABClient/archive/$CRABCLIENTVER.tar.gz | tar zx || exit 2
curl -L https://httplib2.googlecode.com/files/httplib2-0.8.tar.gz | tar zx || exit 2
curl -L http://download.cherrypy.org/cherrypy/3.2.2/CherryPy-3.2.2.tar.gz | tar zx || exit 2
curl -L https://pypi.python.org/packages/source/S/SQLAlchemy/SQLAlchemy-0.8.0.tar.gz | tar zx || exit 2

pushd WMCore-$WMCOREVER/src/python
zip -r $STARTDIR/CRAB3.zip WMCore || exit 3
popd

pushd CAFTaskWorker-$TASKWORKERVER/src/python
zip -r $STARTDIR/CRAB3.zip TaskWorker || exit 3
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


echo 'export PATH=`dirname ${BASH_SOURCE[0]}`:$PATH' > setup.sh

tar zcf $ORIGDIR/TaskManagerRun.tar.gz CRAB3.zip setup.sh crab3 crab || exit 4

popd

