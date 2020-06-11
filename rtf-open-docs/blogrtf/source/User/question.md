常见问题解答
================

* question：我已经下载并安装好了RTF，但是启动之后改变mysql中的数据却没有写到hdfs上？
 >answer：请确保你的所有操作都严格按照[RTF配置](../QuickStart/kafka_properties)中配置，检查包括但不限于mysql的端口号是否跟配置中一致，mysql的binlog是否出于开启状态等。
<br>

* question：从hive表查数时报错"FAILED: RuntimeException java.lang.ClassNotFoundException: com.jd.dw.RTFInputFormat"
 >answer：由于RTF表的存储格式需要特定的Reader进行读取，请检查是否在启动hive之后进行了add jar xxx/RTFReaderV2-2.0-SNAPSHOT.jar 操作。
<br>

* question：RTFWriter终止，查看日志发现内存不足
 >answer：一般造成这种情况的原因有两种，如果文件可以压实可能是compaction_num过大，可以尝试适当减小compaction_num；如果文件无法压实，则极有可能是hdfs上的数据量过大，请尝试清理数据之后再执行。
<br>

* question：使用Kafka版本的RTF无法运行，检查日志后发现"Exception in thread "main" java.lang.NoSuchMethodError: scala.Product.$init$(Lscala/Product;)V"
 >answer：由于在开发的时候使用的spark依赖为2.11.12，请检查自己的spark版本对应的scala版本是否符合要求[查看版本](https://mvnrepository.com/artifact/org.apache.spark/spark-core)


