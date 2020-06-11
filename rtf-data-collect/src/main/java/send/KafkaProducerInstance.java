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

package send;


import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.jd.dw.rtf.binlog.Factory.BinLogFactory;
import com.jd.dw.rtf.binlog.avro.RtfData;
import com.jd.dw.rtf.binlog.bintools.Tools;
import com.jd.dw.rtf.binlog.buffer.RealTimeBuffer;
import com.jd.dw.rtf.binlog.model.KafkaUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author anjinlong
 * @Date 2020/3/19 下午4:57
 * @Version 1.0
 */
public class KafkaProducerInstance {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerInstance.class);
    private static String CONF_FILE_NAME = "conf.properties";
    private static String host;
    private static int port;
    private static String DT_names;
    private static String username;
    private static String password;
    private static String Topics;
    private static String server;

    public static void main(String[] args) throws IOException {
        //propertiesMain为共有配置，propPath为文件夹下各topic配置
        String confBinlog = args[0];
        //String confKafka = args[1]; //存放producer信息及库表信息



        Properties propertiesKafka = new Properties();
        Properties propertiesbinlog = new Properties();


        try {

            propertiesbinlog.load(new FileInputStream(confBinlog));
            host = propertiesbinlog.getProperty("host");
            port = Integer.parseInt(propertiesbinlog.getProperty("port"));
            username = propertiesbinlog.getProperty("username");
            password = propertiesbinlog.getProperty("password");
            server = propertiesbinlog.getProperty("server");
            Topics = propertiesbinlog.getProperty("Topics");
            DT_names = propertiesbinlog.getProperty("DT_names");

/*            host = "10.198.226.201";
            server = "10.198.226.201:9092";
            port = 3306;
            username = "root";
            password = "root";
            DT_names = "test:test_usermessage";
            Topics = "tpUsermessage";*/

            propertiesKafka.put("bootstrap.servers", server);
            propertiesKafka.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
            propertiesKafka.put("value.serializer","com.jd.dw.rtf.binlog.avro.RtfSerializer");

        }catch (Exception e){
            System.out.println(e);
        }

        HashMap<String, String> topicMap = Tools.readTopic(DT_names, Topics);
        if(topicMap == null){
            logger.info("请确保库表名与topic名对应！");
        }

        KafkaUtil.createKafaTopic(server, Topics);

        KafkaProducer<String, RtfData> producer = new KafkaProducer<>(propertiesKafka);
        BinLogFactory binLogFactory = new BinLogFactory(host,port,username,password);
        BinaryLogClient client = binLogFactory.getBinlogClient();
        client.registerEventListener(binLogFactory.getEventListener());

        Thread thread = new Thread(new sendMessage(producer, topicMap));//添加方法
        thread.start();
        System.out.println("启动成功");
        client.connect();
    }

    public static class sendMessage implements Runnable {
        public KafkaProducer<String,RtfData> producer;
        public HashMap<String,String> dbMap;

        public sendMessage(KafkaProducer<String, RtfData> producer, HashMap<String, String> dbMap) {
            this.producer = producer;
            this.dbMap = dbMap;
        }

        @Override
        public void run() {
            while (true){
                if (RealTimeBuffer.queue.isEmpty() == false) {
                    RtfData record = RealTimeBuffer.queue.pollFirst(); //返回queue的第一个元素，如果是空则返回null
                    String dtName = new StringBuffer().append(record.getDb()).append(":").append(record.getTb()).toString();

                    if(dbMap.keySet().contains(dtName)){
                        String topic = dbMap.get(dtName);
                        System.out.println("发送消息给" + topic + "内容是" + record);
                        producer.send(new ProducerRecord<String, RtfData>(topic,record));
                    }
                }else {
                    try {
                        logger.info("sleep for 1 s");
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}