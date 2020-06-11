![](https://img.shields.io/badge/build-passing-brightgreen.svg) ![](https://img.shields.io/badge/license-Apache--2.0-green.svg) ![](https://img.shields.io/badge/Language-Java-important.svg) ![](https://img.shields.io/badge/Language--Version-jdk1.8%2B-yellow.svg)

**1.  RTF简介**
==
京东RTF实时数据湖，是一个从底层重新构建的系统，解决了数据的接入、解析及清洗等ETL 过程，同时解决了传统离线模式达不到的实时性和流式实时数据做不到的数据清洗、还原，是一套大数据领域改革性的实时数据方案。RTF可以直接查询最新状态的数据，并且无需去重，可以让数据分析人员即使不了解flink或spark等实时计算框架，也能够获取实时数据进行分析。

使用RTF需要mysql5.x，hadoop2.0，jdk1.8及以上版本。



**2.  RTF流程说明**
==
![Image text](liucheng.png/liucheng001.png)








---

**3.  工作原理**
==







---



**4.  Quick Start**
==

首先我们需要开启binlog功能，并配置binlog模式为ROW，在my.cnf中配置如下信息

```
[mysqld]
log-bin       = mysql-bin  #  开启binlog
binlog-format = ROW        #  选择模式为ROW
server_id     = 1          #
```

由于我们从binlog读取数据的方式是主从复制，所以需要一个非root用户模仿slave。需要新建用户并授权，如果已经有非root账号可以直接授权

```
CREATE USER canal IDENTIFIED BY ‘rtf_user’;
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO ‘rtf_user’@‘%’;
FLUSH PRIVILEGES;  #  刷新权限
```

下载rtf项目 www://……….

上传到服务器上并解压到指定目录：

```
tar -zxvf rtf-writer-1.0-SNAPSHOT.tar.gz rtf
```

在启动之前，我们需要进入解压之后的目录下的conf，在rtf_writer.properties中配置以下信息：

```
host        =  #mysql所在的主机ip
port        =  #mysql的端口名称，一般默认为3306
dn_names    =  #监听的离线表名，按照db1:table1 db2:table2格式输入
username    =  #之前新建的用户
password    =  #用户的密码
filenames   =  #存放实时数据的hdfs上的文件夹，不同的文件夹需要用空格隔开(需要与dn_names严格一一对应)
primaryKeys =  #每个表的主键信息(需要与dn_names严格一一对应)
allfields   =  #每个表各个字段的信息(需要与dn_names严格一一对应)
```

至此，需要配置的基本信息就已经全部完成了。接下来，需要在hdfs上初始化你希望获得实时数据的离线表的对应文件夹。

```
sh rtf-init.sh
```


你监听的每个DB中的表需要在hive上建一个外部表，并且指向对应的hdfs文件夹，此处应该注意，STORED AS INPUTFORMAT一定需要填RTFInputFormat格式，否则无法读取数据。进入hive之后，执行 add jar rtf/lib/RTFReader.jar。然后执行建表命令，这里以test_usermessage做示范：

```
CREATE EXTERNAL TABLE `test_usermessage_rtf`(
    `id` int,
    `name` string,
    `age` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
STORED AS INPUTFORMAT
  'com.jd.dw.RTFInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'
LOCATION 'hdfs://your_hostname/user/root/rtf_writer_test/';
```

配置好之后进入bin目录下，启动脚本 sh start.sh
进入log中 查看rtf_running.log 是否已经启动成功,如果启动成功，那么恭喜你，你已经可以使用简单的hive命令来实现实时数据的查询了！

改变mysql端的数据之后，在对应的hive表中输入命令：
```
Select * from test_usermessage_rtf limit 10;
```
