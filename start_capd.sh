#!/bin/bash

./kill_capd.sh

# Receive paramters of nfcapd, $1: directory that nfcapd data saved 
if [ $# == 1 ];then
  nfcapd -w -D -T all -l $1
else
  nfcapd -w -D -T all -l /home/victor/data/flow_dir/
fi


