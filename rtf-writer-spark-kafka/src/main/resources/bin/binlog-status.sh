#!/bin/bash

APP_NAME=$(dirname "$PWD")/lib/rtf-data-collect-1.0-SNAPSHOT.jar
LOG_FILE=$(dirname "$PWD")/log/binlog-reading.log

is_exist(){
    echo
    pid=`ps -ef|grep $APP_NAME|grep -v grep|awk '{print $2}' `
    if [ -z "${pid}" ]; then
        return 1
    else
        return 0
    fi
}

status(){
    is_exist
    if [ $? -eq "0" ]; then
        echo "binlog server is running. Pid is ${pid}"
    else
        echo "binlog server is not running, you can start it use binlog-start.sh"
    fi
}

status
