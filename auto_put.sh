#!/bin/bash

# ====

function dump_netflow(){
	# process binary netflow data and save into a text file
	# params: 1: a binary netflow file or a series of binary netflow data files;
	# 	  2: absolute path of nfdump output file 
	# return: whether nfdump executes successfully

	echo nfdump is running...
	if ! test -r $1; then
		echo Error: $1 doesn\'t exists! 
	fi

	if [[ $(echo $1 | grep -E -o ':') == ':' ]]; then
		$(nfdump -R $1 -q -o pipe > $2)
	else
		$(nfdump -r $1 -q -o pipe > $2)
	fi
	if [[ $? == 0 ]]; then
		echo $(date +%Y/%m/%d-%H:%M:%S),nfdump,$1,$2 >> $_ok_log_file
		rename $1 $1_ok $1
		return 1
	else
		echo $(date +%Y/%m/%d-%H:%M:%S),nfdump,$1,$2 >> $_no_log_file
		return 0
	fi
}

function put_hdfs(){
	# put nfdump text file onto hdfs while ensuring if the put operation is successfully done
	# params: 1: nfdump text file
	# 	2: hdfs path to save the text file
	# return: whether put hdfs succeeds or not

	echo putting text files onto hdfs...
	$_hadoop -put file://$1 $2
	if [[ $? == 0 ]]; then
		echo $(date +%Y/%m/%d-%H:%M:%S) puthdfs,$1,$2 >> $_ok_log_file
		#rm -f $1
		rename $1 $1_ok $1
		return 1
	else
		echo $(date +%Y/%m/%d-%H:%M:%S) puthdfs,$1,$2 >> $_no_log_file
	fi
}

function aggregate_all(){
	# aggregate all steps of nfdump, put hdfs
	# params: 1. time string
	# 	2. nfcapd directory
	# 	3. nfdump directory
	#	4. hdfs directory

	nc_file=$2nfcapd.$1
	nd_file=$3nfdump.$1
	dump_netflow $nc_file $nd_file
	if [[ $? != 1 ]]; then 
		echo Error: nfdump $nc_file failed! 
	else
		put_hdfs $nd_file $4
		if [[ $? != 1 ]]; then
			echo Error: put hdfs $nd_file $3 failed!
		else
			echo Info: the whole process of $1 completed! 
		fi
	fi
}


function batch_process(){
	# batch process existing nfcapd files which are waiting until current time
	# params: 1. directory of nfcapd files
	# 	2. directory of nfdump output
	# 	3. directory of hdfs to put text files

	waiting_files=`ls $1 | egrep ^nfcapd.[[:digit:]]+$`
	for i in $waiting_files
	do 
		aggregate_all ${i/nfcapd./} $1 $2 $3
	done
}

# ====

function next_rotate(){
	# calculate the time at which next rotate will begin
	# params: [optional]latest rotate time in the format '%Y%m%d%H%M' or nowtime by default
	# return: time of next rotate

	if [[ $# == 0 ]]; then 
		d_str=`date +%Y%m%d%H%M`
		last_char=${d_str: -1}
		
		if [[ $(($last_char<5)) == 1 ]]; then
			w=-$last_char
		else
			w=-$(($last_char-5))
		fi
	else
		d_str=$1
		w=5
	fi
	secs=$(($w*60))
	format_dstr="${d_str:0:4}-${d_str:4:2}-${d_str:6:2} ${d_str:8:2}:${d_str:10:2}"
	stamp=$(date +%s -d "$format_dstr")
	stamp2=$(($stamp+$secs))
	_n=$(date +%Y%m%d%H%M -d "1970-01-01 UTC $stamp2 seconds")
	echo $_n
}


function schedule_task(){
	# schedule task for periodically dumping and putting netflow data every 5 mins

	init
	nxt_rotate=`next_rotate`
	format_time="${nxt_rotate:0:4}-${nxt_rotate:4:2}-${nxt_rotate:6:2} ${nxt_rotate:8:2}:${nxt_rotate:10:2}"
	stamp=$(date +%s -d "$format_time")
	newcoming_time=$(($stamp+305))

	while :
	do
		batch_process $nc_dir $nd_dir $hdfs_dir
		curr_time=`date +%s`
		if [[ $curr_time < $newcoming_time ]]; then
			sleep $(($newcoming_time-$curr_time))
		fi
		nxt_rotate=`next_rotate $nxt_rotate`
		format_time="${nxt_rotate:0:4}-${nxt_rotate:4:2}-${nxt_rotate:6:2} ${nxt_rotate:8:2}:${nxt_rotate:10:2}"
		stamp=$(date +%s -d "$format_time")
		newcoming_time=$(($stamp+305))
		# break
	done
}


function schedule_task_async(){
	# asynchronously schedule task of periodically collecting data

	init
	batch_process $nc_dir $nd_dir $hdfs_dir &

	# align the clock and the first rotate time
	nxt_time=$(next_rotate)
	format_nxt="${nxt_time:0:4}-${nxt_time:4:2}-${nxt_time:6:2} ${nxt_time:8:2}:${nxt_time:10:2}"
	nxt_stamp=$(date +%s -d "$format_nxt")
	now_stamp=$(date +%s)
	align_secs=$(($nxt_stamp-$now_stamp+60*5+3))
	echo waiting for $nxt_time to be completed...
	sleep $align_secs

	while :
	do
		aggregate_all $nxt_time $nc_dir $nd_dir $hdfs_dir &
		nxt_time=$(next_rotate $nxt_time)
		echo waiting for $nxt_time to be completed...
		sleep 5m
	done
	# wait
}


function init(){
	if ! test -d $nc_dir; then mkdir -p $nc_dir; fi
	if ! test -d $nd_dir; then mkdir -p $nd_dir; fi
	if ! test -w $_ok_log_file; then	touch $_ok_log_file; fi
	if ! test -w $_no_log_file; then touch $_no_log_file; fi
	$_hadoop -test -d $hdfs_dir
	if [[ $? != 0 ]]; then
		$_hadoop -mkdir -p $hdfs_dir
	fi
}


# ==== scripts
# variables below can be re-assigned when another script includes these file
data_dir=/home/victor/data/
nc_dir=${data_dir}flow_dir/
nd_dir=${data_dir}nd_dir/
hdfs_dir=/user/hadoop/flow_txt/

# inner variables that are not recommended to be modified
_ok_log_file=${nc_dir}transaction_ok.log
_no_log_file=${nc_dir}transaction_no.log
_hadoop="/usr/hadoop/hadoop-3.0.0/bin/hadoop fs"


# ==== debug
#echo $(next_rotate)

# schedule_task_async