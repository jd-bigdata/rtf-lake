#!/bin/bash
FILE_LOCATION=$1
FILE_NUM=$2
LOG_FILE=$(dirname "$PWD")/logs/init.log
APP_NAME=$(dirname "$PWD")/lib/HisInit-2.0.jar

start(){
      echo "file_path : ${FILE_LOCATION}"
      echo "file_num : ${FILE_NUM}"
      nohup hadoop jar $APP_NAME $FILE_LOCATION $FILE_NUM >$LOG_FILE 2>&1 &
}

start

while true
do
     pid=`ps -ef|grep $APP_NAME|grep -v grep|awk '{print $2}' `
     if [ -z "${pid}" ]; then
           echo "init finished, please check the ${FILE_LOCATION} to make sure the init is successfully!"
           break;
    else
           sleep 3
           echo "please wait, Initializing......"
    fi
    done
