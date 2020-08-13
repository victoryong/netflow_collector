

# script start here...
# 
####################

# non-schedule

p1=/home/victor/GitRepo/flow_dir/nfcapd.202007292035
p2=/home/victor/GitRepo/nd_dir/
p3=/user/hadoop/flow_txt/

check_sh_params $p1 $p2 $p3

if [[ $? == 0 ]]; then
	exit -1
fi
	

#dump_netflow $p1 $p2
#put_hdfs $p2 $p3
aa=$(next_rotate)

