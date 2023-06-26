#!/bin/bash

#Syntax:
#./mv_sys argument1 argument2
#./mv_sys soc-live-journal1 1
#BASEFILE="soc-live-journal1"
#
BASEFILE="$1"
d="_"

NODE=$(hostname)
#ENV="env1"
ENV="env$2"
DIR="${BASEFILE}${d}${NODE}${d}${ENV}"
#mv sysinfo4_VT.log ${BASEFILE}${d}${NODE}${d}${ENV}/
mv sysinfo4_VT.log ${DIR}/
#ls -al soc-live-journal1_slave1_env1/

#mv jba.txt ${DIR}
tree ${DIR}
