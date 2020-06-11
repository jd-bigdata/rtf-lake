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

package receive;

import com.jd.dw.rtf.binlog.avro.RtfData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaReceive {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.198.226.201:9092");
        props.put("group.id", "test");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // 设置反序列化类为自定义的avro反序列化类
        props.put("value.deserializer","com.jd.dw.rtf.binlog.avro.RtfDeserializer");
        KafkaConsumer<String, RtfData> consumer = new KafkaConsumer<String, RtfData>(props);

        consumer.subscribe(Arrays.asList("tpUsermessage"));

        try {
            while(true) {
                ConsumerRecords<String, RtfData> records = consumer.poll(100);
                for(ConsumerRecord<String, RtfData> record : records) {
                    RtfData stock = record.value();
                    System.out.println(stock.toString());
                }
            }
        }finally {
            consumer.close();
        }
    }
}