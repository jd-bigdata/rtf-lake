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

package com.jd.dw.rtf.writer.lanch;

import com.jd.dw.rtf.writer.processor.HiveKeyword;
import com.jd.dw.rtf.writer.spark.SparkLancherKafka;
import com.jd.dw.rtf.writer.tools.Tools;
import org.apache.commons.lang.StringUtils;

import java.io.FileInputStream;
import java.util.Properties;

import static com.jd.dw.rtf.writer.Constants.*;

/**
 * @author anjinlong
 * @create 2018-05-30 14:12
 * @description description
 **/
public class Lancher {

    private static final String DEFAULT_COMPACTION_NUM = "30";
    private static final String DEFAULT_START_COMPACTION_FACTOR = "10485760";


    private static String filename;
    private static String topicName;
    private static String brokerlist;
    private static String group;
    private static String checkPointDir;
    private static String fields;
    private static String offsetFilename;
    private static String CONF_FILE_NAME = "conf.properties";
    private static String primaryKey;

    private static String[] fieldArray;
    private static String[] primaryKeyArray;
    private static String maxRatePerPartition;
    private static String durations;
    private static String subconcurrency;
    private static String backPressureEnabled;
    private static String backPressureInitialRate;
    private static int maxCompactionNum;
    private static long startCompactionFactor;
    private static int fileCount;
    private static String appName;
    private static String recentlyNDays;
    private static String parallelism;
    private static String executorIdleTimeout;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: Lancher <propertiesNameOnHdfs>\n" +
                    "  <propertiesNameOnHdfs> is a filename of config on hdfs \n\n");
            System.exit(1);
        }
        String propertiesNameOnHdfs = args[0];
        loadPropeties(propertiesNameOnHdfs);
        Tools.printLog("java.class.path = " + System.getProperty("java.class.path"));
        SparkLancherKafka sparkLancher = new SparkLancherKafka(fieldArray,
                primaryKeyArray,
                filename,
                maxRatePerPartition,
                durations,
                subconcurrency,
                backPressureEnabled,
                backPressureInitialRate,
                maxCompactionNum,
                startCompactionFactor,
                fileCount,
                recentlyNDays
        );
        sparkLancher.runSpark(
                topicName,
                brokerlist,
                group,
                checkPointDir,
                appName,
                parallelism,
                executorIdleTimeout
        );
    }

    public static void loadPropeties(String propertiesName) {
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(propertiesName));
            startCompactionFactor = Long.parseLong(properties.getProperty("START_COMPACTION_FACTOR",
                    DEFAULT_START_COMPACTION_FACTOR));
            MUST_COMPACTION_FACTOR = Long.parseLong(properties.getProperty("MUST_COMPACTION_FACTOR"));
            CORE_POOL_SIZE = Integer.parseInt(properties.getProperty("CORE_POOL_SIZE"));
            MAX_POOL_SIZE = Integer.parseInt(properties.getProperty("MAX_POOL_SIZE"));
            CONSUME_TIME = Integer.parseInt(properties.getProperty("CONSUME_TIME"));
            maxCompactionNum = Integer.parseInt(properties.getProperty("MAX_COMPACTION_NUM",
                    DEFAULT_COMPACTION_NUM));

            filename = properties.getProperty("filename");
            topicName = properties.getProperty("topic");
            brokerlist = properties.getProperty("brokerlist");
            group = properties.getProperty("group");

            checkPointDir = properties.getProperty("checkPointDir");
            fields = properties.getProperty("fields").toLowerCase();
            offsetFilename = properties.getProperty("offset_filename");
            primaryKey = properties.getProperty("primary_key").toLowerCase();
            maxRatePerPartition = properties.getProperty("maxRatePerPartition", "20000");
            durations = properties.getProperty("durations", "10");
            subconcurrency = properties.getProperty("subconcurrency", "10");
            backPressureEnabled = properties.getProperty("backPressureEnabled", "false");
            backPressureInitialRate = properties.getProperty("backPressureInitialRate", "0");
            appName = properties.getProperty("appName", "rtf_writer_spark");
            recentlyNDays = properties.getProperty("recently_n_days", MAX_INT);
            parallelism = properties.getProperty("parallelism", "20");
            fileCount = Integer.parseInt(properties.getProperty("fileCount", parallelism));
            executorIdleTimeout = properties.getProperty("executorIdleTimeout", "1800s");

            fieldArray = fields.split(",", -1);
            primaryKeyArray = primaryKey.split(",", -1);

            fieldArray = HiveKeyword.replaceArray(fieldArray);
            primaryKeyArray = HiveKeyword.replaceArray(primaryKeyArray);

        } catch (Exception e) {
            Tools.printLog("load properties error:" + e.getMessage());
            System.exit(1);
        }

        Tools.printLog("START_COMPACTION_FACTOR:" + startCompactionFactor);
        Tools.printLog("MUST_COMPACTION_FACTOR:" + MUST_COMPACTION_FACTOR);
        Tools.printLog("APPEND_FACTOR:" + APPEND_FACTOR);
        Tools.printLog("CORE_POOL_SIZE:" + CORE_POOL_SIZE);
        Tools.printLog("MAX_POOL_SIZE:" + MAX_POOL_SIZE);
        Tools.printLog("CONSUME_TIME:" + CONSUME_TIME);
        Tools.printLog("MAX_COMPACTION_NUM:" + maxCompactionNum);

        Tools.printLog("filename: " + filename);
        Tools.printLog("topicName: " + topicName);
        Tools.printLog("brokerlist: " + brokerlist);
        Tools.printLog("group: " + group);
        Tools.printLog("checkPointDir: " + checkPointDir);
        Tools.printLog("offsetFilename: " + offsetFilename);
        Tools.printLog("fields: " + fields);

        Tools.printLog("fieldArray: " + StringUtils.join(fieldArray, ","));
        Tools.printLog("primaryKeyArray: " + StringUtils.join(primaryKeyArray, ","));
        Tools.printLog("maxRatePerPartition: " + maxRatePerPartition);
        Tools.printLog("durations: " + durations);
        Tools.printLog("subconcurrency: " + subconcurrency);
        Tools.printLog("backPressureEnabled:" + backPressureEnabled);
        Tools.printLog("backPressureInitialRate:" + backPressureInitialRate);
        Tools.printLog("recentlyNDays: " + recentlyNDays);
        Tools.printLog("executorIdleTimeout:" + executorIdleTimeout);

    }

    public static String getOffsetFilename() {
        return offsetFilename;
    }


}