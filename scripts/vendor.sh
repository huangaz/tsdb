#!/bin/bash
OD=${PWD}
VS=${PWD}/cmd
VD=${PWD}/cmd/vendor

if ! [[ "$0" =~ "scripts/vendor.sh" ]]; then
	echo "must be run from repository root"
	exit 255
fi

cd ${VS}

rm -rf ${VD}
mkdir ${VD}
govendor add +external
cd ${VD}/github.com/huangaz
rm -rf tsdb
ln -s ../../../.. tsdb
cd ${OD}
