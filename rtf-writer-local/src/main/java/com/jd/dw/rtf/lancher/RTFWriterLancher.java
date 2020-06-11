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

package com.jd.dw.rtf.lancher;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.jd.dw.rtf.binlog.Factory.BinLogFactory;
import com.jd.dw.rtf.binlog.buffer.RealTimeBuffer;
import com.jd.dw.rtf.binlog.model.RtfMessageType;
//import com.jd.dw.rtf.writer.ConsumeSaved.ConsumerSave;
import com.jd.dw.rtf.writer.ConsumeSaved.ConsumerSave;
import com.jd.dw.rtf.writer.schedule.DirectConsumer;
import com.jd.dw.rtf.writer.schedule.RTDataForSchedule;
import com.jd.dw.rtf.writer.tools.SplitTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author anjinlong
 * @Date 2020/2/24 2:38
 * @Version 1.0
 * <p>
 * java -cp rtf-writer-1.0-SNAPSHOT.jar com.jd.dw.rtf.lancher.RTFWriterLancher 127.0.0.1 3306 root 12345678 1 1 1
 */
public class RTFWriterLancher {

    private static final Logger logger = LoggerFactory.getLogger(RTFWriterLancher.class);
    private static String CONF_FILE_NAME = "conf.properties";
    private static String host;
    private static int port;
    private static String DT_names;
    private static String username;
    private static String password;
    private static String filenames;
    private static String primaryKeys;
    private static String allFields;


    public static void main(String[] args) throws Exception {
        String propertiesName = args[0];
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(propertiesName));
            host = properties.getProperty("host");
            port = Integer.parseInt(properties.getProperty("port"));
            DT_names = properties.getProperty("dn_names");
            username = properties.getProperty("username");
            password = properties.getProperty("password");
            filenames = properties.getProperty("filenames");
            primaryKeys = properties.getProperty("primaryKeys");
            allFields = properties.getProperty("allfields");

            /**
             host = "192.168.210.101";
             port = 3306;
             DT_names = "test:test_usermessage test:test_1";
             username = "canal";
             password = "canal";
             filenames = "hdfs://192.168.210.101:9000//user/root/rtf_writer_test/ hdfs://192.168.210.101:9000//user/root/rtf_writer_test1/";
             primaryKeys = "id id";
             allFields = "id,name,age id,name";
             **/

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }

        ArrayList<String> dtNameList = SplitTools.getNameSplit(DT_names);
        ArrayList<String> fileNameList = SplitTools.getNameSplit(filenames);
        ArrayList<String> primaryKeyList = SplitTools.getNameSplit(primaryKeys);
        ArrayList<String> fieldsList = SplitTools.getNameSplit(allFields);

        BinLogFactory binLogFactory = new BinLogFactory(host, port, username, password);
        BinaryLogClient client = binLogFactory.getBinlogClient();
        client.registerEventListener(binLogFactory.getEventListener());

        Thread thread = new Thread(new RtfWorker(fileNameList, fieldsList, primaryKeyList, dtNameList));
        thread.start();
        logger.info("lunch success!");
        client.connect();
    }

    public static class RtfWorker implements Runnable {

        public ArrayList<String> filenameList;
        public ArrayList<String> fieldsList;
        public ArrayList<String> primaryKeyList;
        public ArrayList<String> dbTableList;

        public RtfWorker(ArrayList<String> filenameList, ArrayList<String> fieldsList
                , ArrayList<String> primaryKeyList, ArrayList<String> dbTableList) {
            this.filenameList = filenameList;
            this.fieldsList = fieldsList;
            this.primaryKeyList = primaryKeyList;
            this.dbTableList = dbTableList;
        }

        @Override
        public void run() {
            //To access several RTF tables, there are several consumers
            ArrayList<DirectConsumer> consumers = null;
            try {
                consumers = new ConsumerSave(filenameList, primaryKeyList, fieldsList).createConsumer();
            } catch (IOException e) {
                e.printStackTrace();
            }

            while (true) {
                if (RealTimeBuffer.queue.isEmpty() == false) {
                    //Returns the first element of the queue, or null if it is empty
                    RtfMessageType record = RealTimeBuffer.queue.pollFirst();
                    try {
                        //Determine whether it is the RTF table to be accessed according to the record's library name
                        // and table name, and then locate which consumer to use
                        StringBuffer dbTable = new StringBuffer().append(record.getDb()).append(":").append(record.getTb());
                        if (SplitTools.tableContains(dbTableList, dbTable)) {
                            int dtIndex = SplitTools.tableFindIndex(dbTableList, dbTable);
                            DirectConsumer consumer = consumers.get(dtIndex);
                            consumer.process(record);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                } else {
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