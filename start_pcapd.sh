#!/bin/sh

data_dir=/home/victor/data/

if [[ $# == 2 ]]; then
	interface=$1
	flow_dir=$2
else
	interface=ens33
	flow_dir=${data_dir}pflow_dir/
fi

nfpcapd -i $interface -l $flow_dir

