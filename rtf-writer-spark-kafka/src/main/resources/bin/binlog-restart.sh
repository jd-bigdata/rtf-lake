#!/bin/bash

APP_NAME=$(dirname "$PWD")/lib/rtf-data-collect-1.0-SNAPSHOT.jar
LOG_FILE=$(dirname "$PWD")/logs/binlog-reading.log
prop_add=$(dirname "$PWD")/conf/binlog.properties

is_exist(){
    echo
    pid=`ps -ef|grep $APP_NAME|grep -v grep|awk '{print $2}' `
    if [ -z "${pid}" ]; then
        return 1
    else
        return 0
    fi
}

start(){
    is_exist
    if [ $? -eq "0" ]; then    # [$? -eq "0"] 说明pid不等于空 说明服务正在运行中，将进程号打印出来
        echo "binlog-server exist, stopping....."
    else
        nohup java -jar $APP_NAME $prop_add >$LOG_FILE 2>&1 &  # 说明pid为空 执行java -jar 命令启动服务
    fi
}

stop(){
    is_exist
    if [ $? -eq "0" ]; then
        kill -9 $pid
    fi
}

restart(){
    stop
    start
}


restart
sleep 1
echo "restart binlog-server successfully!"
