#/bin/sh
. auto_put.sh


root_dir=/home/victor/GitRepo/

nc_dir=${root_dir}pflow_dir/
# nd_dir=${root_dir}nd_dir/
# hdfs_dir=/user/hadoop/flow_txt/

ok_log_file=${nc_dir}transaction_ok.txt
no_log_file=${nc_dir}transaction_no.txt

schedule_task
