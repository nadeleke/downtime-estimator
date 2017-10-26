#!/bin/bash

IP_ADDR=$1
NUM_SPAWNS=$2
SESSION_NAME=$3
tmux new-session -s $SESSION_NAME -n bash -d
for ID in `seq 1 $NUM_SPAWNS`;
do
    echo $ID
    tmux new-window -t $ID
    tmux send-keys -t $SESSION_NAME:$ID 'python event_producer.py '"$IP_ADDR"' '"$ID"'' C-m
done
