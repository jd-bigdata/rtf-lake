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

package com.jd.dw.rtf.binlog.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.security.JaasUtils;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;

public class KafkaUtil {

    public static void createKafaTopic(String brokerList,String topic) {
        Properties properties = new Properties();
        Collection<NewTopic> newTopicList = new ArrayList<>();

        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        AdminClient adminClient = AdminClient.create(properties);
        String[] topics = topic.split(" ");

        for(int i = 0; i < topics.length; i++){
            NewTopic newTopic = new NewTopic(topics[i] ,3 , (short) 1);
            newTopicList.add(newTopic);
        }
        try {
            adminClient.createTopics(newTopicList);
            System.out.println("成功创建topic" );
        } catch (Exception e){
            System.out.println(e);
        }

    }
}