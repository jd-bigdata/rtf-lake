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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * Author : qiaochao
 */
public class JDQConsumerExample {

  /**
   * 测试环境使用:
   * <p>
   * 1. 需要host配置/etc/hosts
   * 192.168.144.108 TEST-JDQ-144108.bdp.jd.local
   * 192.168.144.109 TEST-JDQ-144109.bdp.jd.local
   * 192.168.144.110 TEST-JDQ-144110.bdp.jd.local
   * 192.168.144.111 TEST-JDQ-144111.bdp.jd.local
   * 192.168.144.112 TEST-JDQ-144112.bdp.jd.local
   * <p>
   * 2. 另外需要设置测试环境的KDC地址,见getProperties内部设置
   */
  public static void main(String[] args) {

    String username = args[0];
    String password = args[1];
    String brokerlist = args[2];
    String clientId = args[3];
    String group = args[4];
    String topic = args[5];

    /**
     * 注意:
     * 1. clientId
     * 	    线上环境会验证clientId是否属于当前用户,所有这里的clientId一定要用用户申请下来的clientId
     * 		另外这里的clientId也会授权提速(默认5MB/s),非授权的clientId速度为1KB/s
     * 2. username,password,brokerlist,group
     * 		数据来源为JDQ3.0申请审批后的客户端信息
     * 		brokerlist 对应页面中TOPIC信息标签下的 Brokerlist 数据
     * 		group 对应页面中TOPIC信息标签下的 GROUPID 数据
     * 		username和password 需要从客户端详情页面中的所属应用下查看(点击页面中对应的所属应用连接)
     * 			username 对应 应用域名
     * 			password 对应 accesskey
     *
     */
    Properties props = getProperties(username, password, brokerlist, clientId, group);
    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
    Collection<String> topics = Arrays.asList(topic);

    /**
     * 参考方法:
     *
     * consumer.partitionsFor(topic)
     * 		查询topic的分区信息,当本地没有这个topic的元数据信息的时候会往服务端发送的远程请求
     * 		注意: 没有权限的topic的时候会抛出异常(org.apache.kafka.common.errors.TopicAuthorizationException)
     *
     * consumer.position(new TopicPartition(topic, 0))
     * 		获取下次拉取的数据的offset, 如果没有offset记录则会抛出异常
     *
     * consumer.committed(new TopicPartition(topic, 0))
     * 		获取已提交的位点信息，如果没有查询到则返回null
     *
     * consumer.beginningOffsets(Arrays.asList(new TopicPartition(topic, 0)));
     * consumer.endOffsets(Arrays.asList(new TopicPartition(topic, 0)));
     * consumer.offsetsForTimes(timestampsToSearch);
     * 		查询最小,最大,或者任意时间的位点信息
     *
     * consumer.seek(new TopicPartition(topic, 0), 10972);
     * consumer.seekToBeginning(Arrays.asList(new TopicPartition(topic, 0)));
     * consumer.seekToEnd(Arrays.asList(new TopicPartition(topic, 0)));
     * 		设置offset给消费者拉取消息
     *
     * consumer.assign(Arrays.asList(new TopicPartition(topic, 0)));
     * 		手动分配消费的topic和分区进行消费，这里不会出发group management操作,指定分区消费数据
     * 		和subscribe方法互斥,如果assign 之后或者之后调用subscribe 则会报错，不允许再进行分配，2方法不能一起使用
     *
     * consumer.subscribe(topics);
     * 		自动发布消费的topic进行消费,这里触发group management操作
     * 		和assign方法互斥,如果subscribe 之后或者之后调用assign 则会报错，不允许再进行分配，2方法不能一起使用
     *
     * 注: group management
     * 		根据group 进行topic分区内部的消费rebanlance
     * 		例如消费的topic包含3个分区，启动了4个相同鉴权的客户端消费
     * 				分区0 -- consumer1   分区1 -- consumer2   分区2 --- consumer3    consumer4则会空跑不消费数据
     *         当分区consumer1挂掉的时候则会出现rebalance，之后变为
     *          	分区0 -- consumer2   分区1 -- consumer3   分区2 --- consumer4
     *
     */
    consumer.subscribe(topics);

    try {
      while (!Thread.currentThread().isInterrupted()) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(10000);
        for (ConsumerRecord<byte[], byte[]> record : records) {
          System.out.println("[offset]:" + record.offset() + ", [key]:" + new String(record.key()
          ) + ", [value]:" + new String(record.value()));
        }
        /**
         * 提交位点:支持自动提交和手动提交
         *
         * 自动提交:
         * 		需要设置ConsumerConfig中  enable.auto.commit 为true
         * 		同时可设置提交时间间隔   auto.commit.interval.ms 默认为5000
         *
         * 手动提交:
         * 1. 同步提交位点:
         * 		consumer.commitSync();
         * 		consumer.commitSync(offsets);
         * 2. 异步提交位点:
         * 		consumer.commitAsync();
         * 		consumer.commitAsync(offsetCommitCallback);
         * 		consumer.commitAsync(offsets,offsetCommitCallback);
         */
      }
    } finally {
      /**
       * 注意在不用的时候释放资源
       */
      consumer.close();
    }

  }

  private static Properties getProperties(String username, String password, String brokerlist,
                                          String clientId, String group) {

    Properties props = new Properties();

    /**
     * 注意:
     * 测试环境需要指定测试环境的KDC地址,线上环境不需要指定,默认会找到线上KDC
     * 另外需要按照使用手册配置host
     */
    System.setProperty("java.security.krb5.kdc", "TEST-JDQ-144109.bdp.jd.local");

    /**SASL_PLAINTEXT
     * JDQ 安全相关配置：1.JDQ服务端认证方式为SASL_PLAINTEXT，需要在此设置,默认SASL_PLAINTEXT
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

    /**
     * JDQ 安全相关配置：3.需要查看DEBUG日志的可以设置为true，默认为false
     */
    props.setProperty(ConsumerConfig.JDQ_ENABLE_DEBUG_CONFIG, "false");

    /**
     * kafka配置列表，可参考（版本0.10.0.0）http://kafka.apache.org/0101/documentation.html#newconsumerconfigs
     */
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerlist);
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common" +
            ".serialization.ByteArrayDeserializer");
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common" +
            ".serialization.ByteArrayDeserializer");
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return props;
  }
}