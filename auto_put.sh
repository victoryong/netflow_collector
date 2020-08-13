#!/bin/bash

function is_abdir(){
# check if a string is a valid absolute directory path

s=$(echo $1 | grep -E -o '^\/(\w+\/?)+$')
if [[ ${#s} == `expr length $1` ]]; then return 1; else return 0; fi 
}


function check_sh_params(){
# check if parameters sent from the shell command meet the demand of parsing and transmission
# params: All parameters sent from shell command
# return: Meet the demand or not (true/false)

if [[ $# < 3 ]]; then
	echo There are three paramters required!\($# is not engouth. \)
	return 0
else
	file=$(echo $1 | grep -E -o '^\/(\w+\/)+nfcapd.[0-9]+')

	# echo $file
	if test -r $file;then
		echo hahahhah1 > /dev/null
	else		
		echo $1 isn\'t a nfcapd binary data file. Byebye!
		return 0
	fi
	
	nd_dir=$(echo $2 | grep -E -o '^\/(\w+\/?)+$')
	if test -d $nd_dir; then echo hahahahha2  > /dev/null
	else echo $2 isn\'t a existing directory. Byebye!; return 0; 
	fi

	is_abdir $3
	if [[ $? == 0 ]]; then echo $3 isn\'t a valid directory. Byebye!; return 0
	else echo hahahahhaa3 > /dev/null; return 1
	fi
fi
}


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
	$(nfdump -R $1 -q > $2)
else
	$(nfdump -r $1 -q > $2)
fi
if [[ $? == 0 ]]; then
	echo $(date +%Y/%m/%d-%H:%M:%S),nfdump,$1,$2 >> $ok_log_file
	# rm -f $1
	return 1
else
	echo $(date +%Y/%m/%d-%H:%M:%S),nfdump,$1,$2 >> $no_log_file
	return 0
fi
}

function put_hdfs(){
# put nfdump text file onto hdfs while ensuring if the put operation is successfully done
# params: 1: nfdump text file
# 	  2: hdfs path to save the text file
# return: whether put hdfs succeeds or not

echo putting text files onto hdfs...
cd /usr/hadoop/hadoop-3.0.0/
bin/hadoop fs -put file://$1 $2
if [[ $? == 0 ]]; then
	echo $(date +%Y/%m/%d-%H:%M:%S) puthdfs,$1,$2 >> $ok_log_file
	#rm -f $1
	return 1
else
	echo $(date +%Y/%m/%d-%H:%M:%S) puthdfs,$1,$2 >> $no_log_file
fi
}


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

# align the clock and the first rotate time
nxt_time=$(next_rotate)
format_nxt="${nxt_time:0:4}-${nxt_time:4:2}-${nxt_time:6:2} ${nxt_time:8:2}:${nxt_time:10:2}"
nxt_stamp=$(date +%s -d "$format_nxt")
now_stamp=$(date +%s)
align_secs=$(($nxt_stamp-$now_stamp+60*5+5))
echo waiting for $nxt_time to be completed...
sleep $align_secs

while :
do
	nd_file=${nd_dir}nfdump.$nxt_time
	dump_netflow ${nc_dir}nfcapd.$nxt_time $nd_file
	if [[ $? != 1 ]]; then 
		echo Error: nfdump ${nc_dir}nfcapd.$nxt_time failed! 
	else
		put_hdfs $nd_file ${hdfs_dir}
		if [[ $? != 1 ]]; then
			echo Error: put hdfs $nd_file ${hdfs_dir} failed!
		else
			echo Info: the whole process of $nxt_time completed! 
		fi
	fi
	nxt_time=$(next_rotate $nxt_time)
	echo waiting for $nxt_time to be completed...
	sleep 5m
done
}


function init(){
if ! test -d $nc_dir; then mkdir -p $nc_dir; fi
if ! test -d $nd_dir; then mkdir -p $nd_dir; fi
if ! test -w $ok_log_file; then	touch $ok_log_file; fi
if ! test -w $no_log_file; then touch $no_log_file; fi
}

# variables below can be re-assigned when another script includes these file
root_dir=/home/victor/GitRepo/
nc_dir=${root_dir}flow_dir/
nd_dir=${root_dir}nd_dir/
hdfs_dir=/user/hadoop/flow_txt/

ok_log_file=${nc_dir}transaction_ok.log
no_log_file=${nc_dir}transaction_no.log

# debug
#echo $(next_rotate)

