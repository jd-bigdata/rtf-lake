#!/bin/bash

APP_NAME=$(dirname "$PWD")/lib/rtf-writer-spark-kafka-1.0-SNAPSHOT.jar
LOG_FILE=$(dirname "$PWD")/log/$1.log
prop_add=$(dirname "$PWD")/conf/$1

is_exist(){
    echo
    pid=`ps -ef|grep $prop_add|grep -v grep|awk '{print $2}' `
    if [ -z "${pid}" ]; then
        return 1
    else
        return 0
    fi
}

stop(){
    is_exist
    if [ $? -eq "0" ]; then
        kill -15 $pid
        sleep 2
        is_exist
        if [ $? -eq "0" ]; then
            kill -9 $pid
            echo "${pid} :  rtf-writer of ${prop_add}  stop successfully!"
        else
            echo "${pid} :  rtf-writer of ${prop_add}  stop successfully!"
        fi
    else
        echo "rtf-writer of ${prop_add} is not running"
    fi
}

stop
