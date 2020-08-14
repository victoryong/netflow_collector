#!/bin/sh

root_dir=/home/victor/GitRepo/
if [[ $# == 2 ]]; then
	interface=$1
	flow_dir=$2
else
	interface=ens33
	flow_dir=${root_dir}pflow_dir/
fi

nfpcapd -i $interface -l $flow_dir

