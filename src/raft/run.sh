#!/bin/bash  
# time=$(date "+%Y-%m-%d-%H:%M:%S")
# for((i=1;i<=10;i++));  
# do   
# # go test -run ConcurrentStarts2B >> ./output/$1
# # go test -run 2B >> ./output/${time}.log
# # go test -run Figure8Unreliable2C >> ./output/${time}.log
# # go test -run  SnapshotInstallCrash2D >> ./output/${time}.log
# # go test -run SnapshotInstallUnreliable2D >> ./output/${time}.log
# go test -run 2D >> ./output/${time}.log
# done

nohup python launch.py -np 4 -t 100 2A &
nohup python launch.py -np 4 -t 100 2B &
nohup python launch.py -np 4 -t 100 2C &
nohup python launch.py -np 4 -t 100 2D &