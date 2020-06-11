安装RTF(单机版本)
=================

1、首先我们需要开启binlog功能，并配置binlog模式为ROW，在my.cnf中配置如下信息

```
[mysqld]
log-bin       = mysql-bin  #  开启binlog
binlog-format = ROW        #  选择模式为ROW
server_id     = 1          #
```

由于我们从binlog读取数据的方式是主从复制，所以需要一个非root用户模仿slave。需要新建用户并授权，如果已经有非root账号可以直接授权

```
CREATE USER rtfuser IDENTIFIED BY ‘rtfuser’;
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO ‘rtfuser’@‘%’;
FLUSH PRIVILEGES;  #  刷新权限
```

2、点击下载RTF项目压缩包:[Download](../Download/download.md)

上传到服务器上并解压到指定目录：

```
tar -zxvf rtf-writer-1.0-SNAPSHOT.tar.gz rtf
```

3、在启动之前，我们需要进入解压之后的目录下的conf，在rtf_writer.properties中配置以下信息：

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

至此，需要配置的基本信息就已经全部完成了。接下来，需要在hdfs上初始化你希望获得实时数据的离线表的对应文件夹。请转到[快速开始](./start.md)，有我们为您提供的一个示范。
