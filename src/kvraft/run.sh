#!/bin/bash  
# time=$(date "+%Y-%m-%d-%H:%M:%S")
# for((i=1;i<=10;i++));  
# do   
# # # go test -run ConcurrentStarts2B >> ./output/$1
# # # go test -run  2A >> ./output/${time}.log
# # # go test -run SnapshotInstall2D >> ./output/${time}.log
# go test -run 3B -race >> ./output/${time}.log
# done

nohup python launch.py -np 4 -t 100 3B &
