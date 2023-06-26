#!/bin/bash

file="$1"
    
if [ -z "$1" ]
  then
    echo "No argument supplied"
  else
    zip -r -f $file .
fi
