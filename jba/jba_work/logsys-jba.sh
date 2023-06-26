#!/bin/sh

#file_name: logsys-jba.sh
#------------------------
# syntax: ./logsys_jba.sh slave1

NODE="$1"
# Run the script to NODE:
# watch - execute a program periodically, showing output fullscreei
# -n specify update interval in seconds i.e 0.5 
ssh -t $NODE sudo watch -n 6 ./logsysinfo2.sh enp0s3

