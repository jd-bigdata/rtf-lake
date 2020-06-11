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

stop(){
    is_exist
    if [ $? -eq "0" ]; then
        kill -9 $pid
        echo "binlog-server stopped, the change of mysql will not be obtain anymore"
    else
        echo "binlog-server is not running"
    fi
}

stop
