#!/bin/bash

APP_NAME=$(dirname "$PWD")/lib/rtf-writer-spark-kafka-1.0-SNAPSHOT.jar
prop_add=$(dirname "$PWD")/conf/$1
LOG_FILE=$(dirname "$PWD")/logs/$1.log

is_exist(){
    echo
    pid=`ps -ef|grep $prop_add|grep -v grep|awk '{print $2}' `
    if [ -z "${pid}" ]; then
        return 1
    else
        return 0
    fi
}

status(){
    is_exist
    if [ $? -eq "0" ]; then
        echo "rtf-writer of ${prop_add} is running. Pid is ${pid}"
    else
        echo "rtf-writer of ${prop_add} not running"
    fi
}

status
