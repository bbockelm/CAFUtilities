#! /bin/sh

set -x

ORIGDIR=$PWD
STARTDIR=$PWD/tmp/runtime
WMCOREDIR=$STARTDIR/WMCore
WMCOREVER=0.9.58
WMCOREREPO=dmwm
TASKWORKERDIR=$STARTDIR/TaskWorker
TASKWORKERVER=0.1-dagman
TASKWORKERREPO=bbockelm

rm -rf $STARTDIR

mkdir -p $WMCOREDIR
mkdir -p $TASKWORKERDIR

pushd $STARTDIR

curl -L https://github.com/$WMCOREREPO/WMCore/archive/$WMCOREVER.tar.gz | tar zx
curl -L https://github.com/$TASKWORKERREPO/CAFTaskWorker/archive/$TASKWORKERVER.tar.gz | tar zx

pushd WMCore-$WMCOREVER/src/python
zip -r $STARTDIR/WMCore.zip WMCore
popd
pushd CAFTaskWorker-$TASKWORKERVER/src/python
zip -r $STARTDIR/TaskWorker.zip TaskWorker
popd

tar zcf $ORIGDIR/TaskManagerRun.tar.gz WMCore.zip TaskWorker.zip

popd

