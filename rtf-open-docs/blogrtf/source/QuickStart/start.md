快速开始
=============
我已经完成了[RTF的kafka安装](./kafka_properties.md)或[RTF的单机安装](./single_properties.md)

在完成RTF项目的安装与配置之后，下面讲解一下RTF表如何接入及使用：
1、首先我们需要初始化HDFS上的文件夹，进入RTF的bin目录下,执行完之后可以去对应的目录下查看是否生成了后缀名为rtf的文件，如果生成那么这一步就成功了。
```
#单机版本
sh empty-init.sh 参数1（rtf表存放的hdfs路径） 参数2（需要初始化的文件个数）

#kafka版本
sh empty-init.sh 参数1（该rtf表配置文件路径,db1_table1.properties） 参数2（需要初始化的文件个数）
```

2、接着，你监听的每个DB中的表需要在hive上建一个外部表，并且指向对应的hdfs文件夹，此处应该注意，STORED AS INPUTFORMAT一定需要填RTFInputFormat格式，否则无法读取数据。进入hive之后，执行
```
add jar RTFReaderV2-2.0-SNAPSHOT.jar（确保hive所在机器上有这个jar包，在lib目录下有）
```
然后执行建表命令，具体建表语句可以仿照DB中的表：
```
show create table usermessage_rtf;
```
这里以test_usermessage做示范：

```
CREATE EXTERNAL TABLE `test_usermessage_rtf`(
    `id` int,
    `name` string,
    `age` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS INPUTFORMAT
  'com.jd.dw.RTFInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'
LOCATION 'hdfs://your_hostname/user/root/rtf_writer_test/';
```

3、配置好之后进入bin目录下，启动脚本，

```
#kafka版本
sh binlog-start.sh
sh rtf-writer-start.sh 参数1（位于conf下的rtf-writer配置文件,直接写文件名即可）

#单机版本
sh start.sh
```
进入log中 查看rtf_running.log 是否已经启动成功,如果启动成功，那么恭喜你，你已经可以使用简单的hive命令来实现实时数据的查询了！

改变mysql端的数据之后，在对应的hive表中输入命令：
```
Select * from test_usermessage_rtf limit 10;
```
查询成功！

单机版本无法保证面对大量数据的积压处理，也无法保证rtf-writer挂掉之后能够恢复位点，推荐只作为测试和小批量数据的使用，如果涉及到生产环境请使用kafka版本。
