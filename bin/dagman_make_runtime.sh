#! /bin/sh

set -x

ORIGDIR=$PWD
STARTDIR=$PWD/tmp/runtime

WMCOREDIR=$STARTDIR/WMCore
WMCOREVER=0.9.58-dagman
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

rm -rf $STARTDIR

mkdir -p $WMCOREDIR
mkdir -p $TASKWORKERDIR
mkdir -p $DBSDIR
mkdir -p $DLSDIR

pushd $STARTDIR

curl -L https://github.com/$WMCOREREPO/WMCore/archive/$WMCOREVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$TASKWORKERREPO/CAFTaskWorker/archive/$TASKWORKERVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$DBSREPO/DBS/archive/$DBSVER.tar.gz | tar zx || exit 2
curl -L https://github.com/$DLSREPO/DLS/archive/$DLSVER.tar.gz | tar zx || exit 2

pushd WMCore-$WMCOREVER/src/python
zip -r $STARTDIR/WMCore.zip WMCore || exit 3
popd

pushd CAFTaskWorker-$TASKWORKERVER/src/python
zip -r $STARTDIR/TaskWorker.zip TaskWorker || exit 3
popd

pushd DBS-$DBSVER/Clients/Python
zip -r $STARTDIR/DBSAPI.zip DBSAPI || exit 3
popd

pushd DLS-$DLSVER/Client/LFCClient
cp *.py $STARTDIR/ || exit 3
popd

tar zcf $ORIGDIR/TaskManagerRun.tar.gz WMCore.zip TaskWorker.zip  DBSAPI.zip *.py || exit 4

popd

