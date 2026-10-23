#!/bin/bash

PROJECT_DIR=$1
PROJECT_NAME=$2
VERSION=$3
# CPack appends the distribution suffix for CE packages.
RELEASE=$4

export BUILD_NUMBER=${4}

CURDIR=$PWD
TOP_DIR=`pwd`/../

OB_DISABLE_LSE_OPTION=""
[[ $OB_DISABLE_LSE == "1" ]] && OB_DISABLE_LSE_OPTION="-DOB_DISABLE_LSE=ON"

echo "[BUILD] args: TOP_DIR=${TOP_DIR} RELEASE=${RELEASE} BUILD_NUMBER=${BUILD_NUMBER} ${OB_DISABLE_LSE_OPTION}"

cd ${TOP_DIR}
./tools/upgrade/gen_obcdc_compatiable_info.py
./build.sh clean
./build.sh                       \
    rpm                          \
    -DBUILD_CDC_ONLY=ON          \
    -DOB_RELEASEID=$RELEASE      \
    -DBUILD_NUMBER=$BUILD_NUMBER \
    ${OB_DISABLE_LSE_OPTION}     \
    --init                       \
    --make rpm || exit 1

cd ${TOP_DIR}/build_rpm
mv *cdc*.rpm $CURDIR || exit 2
