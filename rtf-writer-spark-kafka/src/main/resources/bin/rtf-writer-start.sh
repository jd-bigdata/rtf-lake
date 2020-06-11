#!/bin/bash
source $(dirname "$PWD")/conf/config.properties
APP_NAME=$(dirname "$PWD")/lib/rtf-writer-spark-kafka-1.0-SNAPSHOT.jar
prop_add=$(dirname "$PWD")/conf/$1
DB_TB=${prop_add##*/}
LOG_FILE=$(dirname "$PWD")/logs/${DB_TB%.*}.log
BIN_LOG=$(dirname "$PWD")/lib/rtf-data-collect-1.0-SNAPSHOT.jar

is_exist(){
    pid1=`ps -ef|grep $prop_add|grep -v grep|awk '{print $2}' `
        if [ -z "${pid1}" ]; then
                return 1
        else
                return 0
        fi
}

binlog_exist(){
    pid=`ps -ef|grep $BIN_LOG|grep -v grep|awk '{print $2}' `
        if [ -z "${pid}" ]; then
                echo "binglog-producer server haven't start, you can start it! "
        else
                echo "binlog-producer server is running, you can use the RTF now! "
        fi
}

start(){
    is_exist    
    if [ $? -eq "0" ]; then    # [$? -eq "0"] 说明pid不等于空 说明服务正在运行中，将进程号打印出
        echo "${APP_NAME} ${prop_add} running. pid=${pid} please kill it first"
    else
        nohup ${SPARK_HOME}/spark-submit --master ${MASTER} --conf spark.cores.max=${SPARK_CORE} $APP_NAME $prop_add >$LOG_FILE 2>&1 &  # 说明pid为空 执行java -jar 命令启动服务
        echo "rtf-writer of ${DB_TB%.*} start successfully!"
    fi
}

start
binlog_exist
