#!/bin/sh

port_report=`netstat -anp | grep 9995`
port_status=$(echo $port_report | grep -E -o '[[:digit:]]+\/nfcapd')
if [[ $port_status != '' ]]; then
	pid=${port_status//\/nfcapd/''}
	kill -9 $pid
fi

