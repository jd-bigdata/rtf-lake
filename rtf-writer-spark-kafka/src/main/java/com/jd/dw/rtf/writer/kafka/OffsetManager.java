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

package com.jd.dw.rtf.writer.kafka;

import com.jd.dw.rtf.writer.tools.Tools;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class OffsetManager   {

  private static KafkaConsumer<byte[], byte[]> consumer = null;

  private static String topicName = null;

  private static OffsetManager offsetManager = null;


  public static void init(String topicName,Properties props){
    OffsetManager.topicName = topicName;
    consumer=new KafkaConsumer<byte[], byte[]>(props);
  }

  public static OffsetManager getInstance()  {
    if (consumer==null || topicName==null){
      throw new RuntimeException("Initialization required");
    }
    if (offsetManager == null) {
      Class var0 = OffsetManager.class;
      synchronized(OffsetManager.class) {
        if (offsetManager == null) {
          offsetManager = new OffsetManager();
        }
      }
    }

    return offsetManager;
  }

  private Map<TopicPartition, OffsetAndMetadata> getOffsets(Map<Integer, Long> partitionOffsets){
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition,
            OffsetAndMetadata>();
    for (int partition : partitionOffsets.keySet()) {
      TopicPartition topicPartition = new TopicPartition(topicName, partition);
      OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(partitionOffsets.get
              (partition));
      offsets.put(topicPartition, offsetAndMetadata);
//      Tools.printLog("topic:" + topicName + ", partition:" + partition + ",offset:" +
//              partitionOffsets.get(partition));
    }
    return offsets;
  }

  public void commitOffset(Map<Integer, Long> partitionOffsets){
    Map<TopicPartition, OffsetAndMetadata> offsets = getOffsets(partitionOffsets);
    consumer.commitSync(offsets);
  }


  public void commitOffsetAsync(HashMap<Integer, Long> partitionOffsets) {
    Map<TopicPartition, OffsetAndMetadata> offsets = getOffsets(partitionOffsets);
    consumer.commitAsync(offsets, new OffsetCommitCallback() {
      public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
        Tools.printLog("Asynchronous commit offset succeeded");
      }
    });

  }
}