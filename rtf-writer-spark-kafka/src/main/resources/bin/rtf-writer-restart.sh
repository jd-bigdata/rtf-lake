#!/bin/bash
source $(dirname "$PWD")/conf/config.properties
APP_NAME=$(dirname "$PWD")/lib/rtf-writer-spark-kafka-1.0-SNAPSHOT.jar
prop_add=$(dirname "$PWD")/conf/$1
DB_TB=${prop_add##*/}
LOG_FILE=$(dirname "$PWD")/logs/${DB_TB%.*}.log

is_exist(){
    echo
    pid=`ps -ef|grep $prop_add|grep -v grep|awk '{print $2}' `
    if [ -z "${pid}" ]; then
        return 1
    else
        return 0
    fi
}

start(){
    is_exist
    if [ $? -eq "0" ]; then 
        echo "rtf-writer of ${DB_TB%.*} is running. pid=${pid}, stopping......"
        pid1=`ps -ef|grep $prop_add|grep -v grep|awk '{print $2}' `
        kill -15 $pid1
        sleep 5
        nohup ${SPARK_HOME}/spark-submit --master ${MASTER} --conf spark.cores.max=${SPARK_CORE} $APP_NAME $prop_add >$LOG_FILE 2>&1 &
    else
        echo "you haven't start rtf-writer of ${DB_TB%.*}, but it will be start soon"
        nohup ${SPARK_HOME}/spark-submit --master ${MASTER} --conf spark.cores.max=${SPARK_CORE} $APP_NAME $prop_add >$LOG_FILE 2>&1 &  
    fi
    is_exist
    if [ $? -eq "0" ]; then
        pid2=`ps -ef|grep $prop_add|grep -v grep|awk '{print $2}' `
        echo "rtf-writer of  ${DB_TB%.*} restart successfully! ------- pid is $pid"
    else
        echo "restart of ${DB_TB%.*} failed, please try again or use stop and start Respectively"
    fi
}


restart(){
    echo "rtf-writer of ${DB_TB%.*} is restarting....."
    start
}


restart
