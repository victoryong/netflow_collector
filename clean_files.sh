#!/bin/sh

rm -f /home/victor/GitRepo/flow_dir/*
rm -f /home/victor/GitRepo/nd_dir/*

cd /usr/hadoop/hadoop-3.0.0/
bin/hadoop fs -rm /user/hadoop/flow_txt/*


