/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.dw.rtf.writer.Testing;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerSiteSeekBytimeAuth {
  private static final Logger logger = LoggerFactory.getLogger(Logger.class);
  private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
  private static String NOW = "now";
  private static String BEFORE = "before";

  public static void main(String args[]) {
// JDQ_ENV.authClinetNV("null", 80);//线下环境测试的时候如果鉴权的时候爆出：找不到CLIRNT_ID的时候加上这个试试
    String topic = args[0];
    String clientId = args[1];//clientId
    String username = args[2];//用户名
    String password = args[3];//密码
    String brokerlist = args[4];//集群ip列表
    String group = args[5];//group
    String timeStamp = args[6];//查询offset的时间(timeStamp取值类型为：{now,before,"yyyy-MM-dd HH:mm"格式的时间})
    System.out.println("Reset Offsets timeStamp: " + timeStamp);
    Properties props = getProperties(username, password, brokerlist, clientId, group);
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);

    List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
    List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
    //打印出来该topic的分区列表
    System.out.println("topic: " + topic + " 分区信息:");
    for (PartitionInfo part : partitionInfos) {
      topicPartitions.add(new TopicPartition(part.topic(), part.partition()));
      System.out.println(part.topic() + " :　" + part.partition());
    }
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition,
            OffsetAndMetadata>();
    //查询最新位点信息
    if (timeStamp.equals(NOW)) {
      Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
      for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
        offsets.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
        System.out.println("EndOffsets offset: " + entry.getKey() + ":" + entry.getValue());
      }
      //查询最小位点信息
    } else if (timeStamp.equals(BEFORE)) {
      Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitions);
      for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
        offsets.put(entry.getKey(), new OffsetAndMetadata(entry.getValue()));
        System.out.println("Beginning offset: " + entry.getKey() + ":" + entry.getValue());
      }
    } else {
      long resetOffsetTime = 0;
      try {
        resetOffsetTime = sdf.parse(timeStamp).getTime();
      } catch (ParseException e) {
        e.printStackTrace();
      }
      System.out.println("resetOffsetTime: " + resetOffsetTime);
      Map<TopicPartition, Long> timestampsToSearch = new HashMap<TopicPartition, Long>();
      for (PartitionInfo part : partitionInfos) {
        timestampsToSearch.put(new TopicPartition(topic, part.partition()), resetOffsetTime);
      }
      //根据时间查询位点信息
      Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes = consumer.offsetsForTimes
              (timestampsToSearch);
      if (offsetsForTimes != null && offsetsForTimes.size() > 0) {
        System.out.println("offsetsForTimes: " + offsetsForTimes);
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetsForTimes.entrySet()) {
          if (entry.getValue() != null) {
            offsets.put(entry.getKey(), new OffsetAndMetadata(entry.getValue().offset()));
            System.out.println("OffsetAndTimestamp: " + entry.getKey() + "find offset: " + entry
                    .getValue().offset());
          } else {
            System.out.println("Find Offsets which time is " + timeStamp + " and partition is " +
                    entry.getKey() + "from kafka is null.");
          }
        }
      } else {
        System.out.println("Find Offsets which time is " + timeStamp + "from kafka is null.");
      }
    }

    /**
     * 提交位点注意：此时提交的位点
     * 分为同步提交位点和异步提交位点
     * commitAsync(Map<TopicPartition, OffsetAndMetadata> offset,OffsetCommitCallback callback)
     *commitSync(Map<TopicPartition, OffsetAndMetadata> offset)
     *
     */
    consumer.commitSync(offsets);
    logger.info("commit offset is ok ~");
    System.out.println("After commited the offsets check :");
    for (PartitionInfo part : partitionInfos) {
      System.out.println(topic + "-" + part.partition() + ":" + consumer.committed(new
              TopicPartition(part.topic(), part.partition())).offset());
    }

  }


  private static Properties getProperties(String username, String password, String brokerlist,
                                          String clientId, String group) {
    Properties props = new Properties();
    /**
     * 测试环境需要指定测试环境的KDC地址,线上环境不需要指定,默认会找到线上KDC,注意这里
     */
// System.setProperty("java.security.krb5.kdc", "TEST-JDQ-144109.bdp.jd.local");

    /**SASL_PLAINTEXT
     * JDQ 安全相关配置：1.JDQ服务端认证方式为SASL_PLAINTEXT，需要在此设置,默认PLAINTEXT
     */
    props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    /**
     * JDQ 安全相关配置：
     *
     * 2.需要走其他方式如keytab访问服务的可设置为true，需要在系统参数中设置krb5和jaas
     * client配置，服务采用的kerberos认证 如果使用SDK，可以设置用户名和密码进行访问，设置方式如下
     */
    props.setProperty(ConsumerConfig.JDQ_USE_KEYTAB_CONFIG, "false");
    props.setProperty(ConsumerConfig.JDQ_PRINCIPAL_NAME_CONFIG, username);
    props.setProperty(ConsumerConfig.JDQ_PASSWORD_CONFIG, password);
    props.setProperty(CommonClientConfigs.ENABLE_FORCE_REFRESH_CONFIG, "false");
    //是否强制刷新．当在线下测试的时候就设置为false
    /**
     * JDQ 安全相关配置：3.需要查看DEBUG日志的可以设置为true，默认为false
     */
    props.setProperty(ConsumerConfig.JDQ_ENABLE_DEBUG_CONFIG, "false");
    /**
     * kafka配置列表，可参考（版本0.10.0.0）http://kafka.apache.org/documentation.html#newconsumerconfigs
     */
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist);
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common" +
            ".serialization.ByteArrayDeserializer");
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common" +
            ".serialization.ByteArrayDeserializer");
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");//不自动提交位点信息
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }
}