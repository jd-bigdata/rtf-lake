#!/bin/bash

APP_NAME=$(dirname "$PWD")/lib/rtf-writer-1.0-SNAPSHOT.jar
LOG_FILE=$(dirname "$PWD")/log/rtf_running.log

is_exist(){
    echo
    pid=`ps -ef|grep $APP_NAME|grep -v grep|awk '{print $2}' `
    if [ -z "${pid}" ]; then
        return 1
    else
        return 0
    fi
}

stop(){
    is_exist
    if [ $? -eq "0" ]; then
        kill -9 $pid
        echo "rtf-writer-local stopped"
    else
        echo "rtf-writer-local is not running"
    fi
}

stop
