![](https://img.shields.io/badge/build-passing-brightgreen.svg) ![](https://img.shields.io/badge/license-Apache--2.0-green.svg) ![](https://img.shields.io/badge/Language-Java-important.svg) ![](https://img.shields.io/badge/Language--Version-jdk1.8%2B-yellow.svg)

**1.  RTF简介**
==
京东RTF实时数据湖，是一个从底层重新构建的系统，解决了数据的接入、解析及清洗等ETL 过程，同时解决了传统离线模式达不到的实时性和流式实时数据做不到的数据清洗、还原，是一套大数据领域改革性的实时数据方案。RTF可以直接查询最新状态的数据，并且无需去重，可以让数据分析人员即使不了解flink或spark等实时计算框架，也能够获取实时数据进行分析。

使用RTF需要mysql5.x，hadoop2.0，jdk1.8及以上版本。

如果您想对于RTF-lake有更多的了解，或者想联系我们，请访问RTF文档：https://rtf-docs.readthedocs.io/en/master/



**2.  RTF流程说明**
==
![Image text](liucheng.png/liucheng001.png)








---

**3.  RTF发布版本信息**
==
a.单机版本：https://github.com/jd-bigdata/rtf-lake/releases/tag/V1.0


b.正式版本：https://github.com/jd-bigdata/rtf-lake/releases/tag/V1.0.0

我们强烈建议您使用正式版本，比单机版本更稳定，性能更好，并且能够在更短的时间处理海量数据。





---



**4.  Quick Start**
==

根据使用的版本请参考配置方式

单机版本：https://rtf-docs.readthedocs.io/en/master/QuickStart/single_properties.html

正式版本：https://rtf-docs.readthedocs.io/en/master/QuickStart/kafka_properties.html
