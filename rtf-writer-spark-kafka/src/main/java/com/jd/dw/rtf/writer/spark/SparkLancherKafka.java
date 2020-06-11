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

package com.jd.dw.rtf.writer.spark;

import com.google.common.base.Strings;
import com.jd.dw.rtf.binlog.avro.RtfData;
import com.jd.dw.rtf.writer.Constants;
import com.jd.dw.rtf.writer.compaction.CompactionWorker;
import com.jd.dw.rtf.writer.hdfs.HDFSTools;
import com.jd.dw.rtf.writer.kafka.KafkaUtil;
import com.jd.dw.rtf.writer.kafka.OffsetTools;
import com.jd.dw.rtf.writer.lanch.Lancher;
import com.jd.dw.rtf.writer.kafka.OffsetManager;
import com.jd.dw.rtf.writer.partition.FilePartitionInfoCache;
import com.jd.dw.rtf.writer.tools.*;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.jd.dw.rtf.writer.Constants.*;

/**
 * @author anjinlong
 * @create 2018-05-30 14:20
 * @description description
 **/
public class SparkLancherKafka implements Serializable {
    private static final long serialVersionUID = 3939220793378098531L;

    private String[] fieldArray;
    private String[] primaryKeyArray;

    private volatile FilePartitionInfoCache filePartitionInfoCache;
    private String maxRatePerPartition;
    private int durations;
    private String subconcurrency;
    private String targetHdfsDir;
    private String backPressureEnabled = "false";
    private String backPressureInitialRate = "0";
    private int maxCompactionNum;
    private long startCompactionFactor;
    private String recentlyNDays;

    private int fileCount;

    public SparkLancherKafka(
            String[] fieldArray,
            String[] primaryKeyArray,
            String hdfsDir,
            String maxRatePerPartition,
            String durations,
            String subconcurrency,
            String backPressureEnabled,
            String backPressureInitialRate,
            int maxCompactionNum,
            long startCompactionFactor,
            int fileCount,
            String recentlyNDays
    ) {
        this.fieldArray = fieldArray;
        this.primaryKeyArray = primaryKeyArray;
        this.targetHdfsDir = hdfsDir;
        this.maxRatePerPartition = maxRatePerPartition;
        this.durations = Integer.parseInt(durations);
        this.filePartitionInfoCache = new FilePartitionInfoCache(hdfsDir);
        this.subconcurrency = subconcurrency;
        this.backPressureEnabled = backPressureEnabled;
        this.backPressureInitialRate = backPressureInitialRate;
        this.maxCompactionNum = maxCompactionNum;
        this.startCompactionFactor = startCompactionFactor;
        this.fileCount = fileCount;
        this.recentlyNDays = recentlyNDays;
    }

    public  SparkConf getSparkConf(String appName, String parallelism, String executorIdleTimeout){
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[*]");
        conf.set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition);
        conf.set("spark.streaming.backpressure.enabled", backPressureEnabled);
        conf.set("spark.streaming.backpressure.initialRate", backPressureInitialRate);
        conf.set("spark.default.parallelism", parallelism);
        conf.set("spark.dynamicAllocation.executorIdleTimeout", executorIdleTimeout);
        conf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        return conf;
    }

    public static  void initOffset(String offsetFileName, String brokerlist, String groupId, String topicName)throws Exception{

        final Map<Integer, Long> partitionOffsets = OffsetTools.readOffsetFileFromHdfs
                (offsetFileName);
        Properties props = KafkaUtil.getProperties(brokerlist, groupId);

        OffsetManager.init(topicName, props);
        if (partitionOffsets.size() > 0) {
            OffsetManager.getInstance().commitOffset(partitionOffsets);
            Tools.printLog("init offset success .");
        }
    }

    /**
     * @param topicName
     * @param brokerlist
     * @param groupId
     * @param checkPointDir
     * @throws Exception
     */
    public void runSpark(String topicName,
                         String brokerlist,
                         String groupId,
                         String checkPointDir,
                         String appName,
                         String parallelism,
                         String executorIdleTimeout) throws Exception {
        JavaSparkContext jsc = new JavaSparkContext(getSparkConf(appName, parallelism, executorIdleTimeout));
        final JavaStreamingContext jssc = new JavaStreamingContext(jsc, Durations.seconds(durations));
        jssc.checkpoint(checkPointDir);
        final Map<Integer, AtomicReference<OffsetRange[]>> offsetRangesMap = new HashMap<Integer,
                AtomicReference<OffsetRange[]>>();
        final String offsetFileName = Lancher.getOffsetFilename();
        initOffset(offsetFileName, brokerlist, groupId, topicName);
        JavaInputDStream<ConsumerRecord<String, RtfData>> stream = lanch(
                jssc,
                topicName,
                brokerlist,
                groupId);
        JavaPairDStream<String, RtfData> stringRtfMessageTypeDStream = stream.transformToPair(
                new Function<JavaRDD<ConsumerRecord<String, RtfData>>, JavaPairRDD<String, RtfData>>() {
                    @Override
                    public JavaPairRDD<String, RtfData> call(JavaRDD<ConsumerRecord<String, RtfData>>
                                                                     rdd) throws
                            Exception {
                        int id = rdd.id();
                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
                        offsetRanges.set(offsets);
                        offsetRangesMap.put(id, offsetRanges);
                        JavaPairRDD<String, RtfData> rs = rdd.mapToPair(new PairFunction<ConsumerRecord<String, RtfData>, String, RtfData>() {
                            @Override
                            public Tuple2<String, RtfData> call(ConsumerRecord<String, RtfData> record)
                                    throws Exception {
                                return new Tuple2<String, RtfData>(record.key(), record.value());
                            }
                        });
                        return rs;
                    }
                }
        );


        JavaDStream<String> lines = stringRtfMessageTypeDStream.map(new Function<Tuple2<String, RtfData>,
                String>() {

            @Override
            public String call(Tuple2<String, RtfData> stringRtfDataTuple2) throws Exception {
                return processData(stringRtfDataTuple2._2());
            }
        });

        JavaPairDStream<String, String> pairLists =
                lines.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, String>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(Iterator<String> s) throws Exception {

                        ArrayList<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>();
                        while (s.hasNext()) {
                            String oneRecord = s.next();
                            String[] ones = oneRecord.split(SPLIT_TAB);
                            String key = ones[0];
                            String filename = findFile(key);
                            try {
                                result.add(new Tuple2<String, String>(filename, oneRecord));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        return result.iterator();
                    }
                });

        JavaPairDStream<String, String> reduceByKeyDStream = pairLists.reduceByKey(new Function2<String,
                String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s + s2;
            }
        }, fileCount);

        JavaPairDStream<Text, Text> linesListsText = reduceByKeyDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, Text, Text>() {
                    @Override
                    public Tuple2<Text, Text> call(Tuple2<String, String> ori) throws Exception {
                        return new Tuple2<Text, Text>(new Text(ori._1), new Text(ori._2));
                    }
                });

        linesListsText.foreachRDD(new VoidFunction<JavaPairRDD<Text, Text>>() {
            private TopNFileList topNFileNames = null;
            @Override
            public void call(final JavaPairRDD<Text, Text> javaPairRDD) throws Exception {
                List<String> topNFileNameList = TopNFileManager.getTopNFileNameList(targetHdfsDir,
                        maxCompactionNum, startCompactionFactor);
                topNFileNames = new TopNFileList(topNFileNameList.toString());
                javaPairRDD.foreach(new VoidFunction<Tuple2<Text, Text>>() {
                    @Override
                    public void call(Tuple2<Text, Text> tuple2) throws Exception {
                        String filenameWithPath = targetHdfsDir + "/" + tuple2._1.toString();
                        String finalPath = targetHdfsDir + "/" + tuple2._1.toString();
                        StringBuffer dataStringBuffer = new StringBuffer(tuple2._2.toString());
                        try {
                            long startTime = System.currentTimeMillis();
                            HDFSTools.appendRTDataByList(filenameWithPath, dataStringBuffer, 8192);
                            StringBuffer msg = new StringBuffer(javaPairRDD.id() + " write success: ")
                                    .append(tuple2._1.toString())
                                    .append(" duration: ").append(System.currentTimeMillis() - startTime);
                            Tools.printLog(msg.toString());

                        } catch (Exception e) {
                            e.printStackTrace();
                            dealWriteException( filenameWithPath, dataStringBuffer);
                        }

                        try {
                            if (topNFileNames.toString().contains(tuple2._1.toString())) {//&&
                                if (!recentlyNDays.equals(MAX_INT)) {
                                    String lastNDate = Tools.getDateBefore(Integer.parseInt(recentlyNDays));
                                    CompactionWorker.doCompactionRecent(finalPath, lastNDate);
                                } else {
                                    CompactionWorker.doCompaction(finalPath);
                                }
                            }
                        } catch (Exception e) {
                            Tools.printLog("***compaction exception !! : " + finalPath +" . "+e.getMessage());
                            e.printStackTrace();
                        }
                    }
                });

                HashMap<Integer, Long> partitionOffsetMap = new HashMap<Integer, Long>();
                int minKey = Integer.MAX_VALUE;
                for (int key : offsetRangesMap.keySet()) {
                    Tools.printLog(" key: " + key);
                    if (key < minKey) {
                        minKey = key;
                    }
                }
                Tools.printLog(" minKey: " + minKey);
                for (OffsetRange o : offsetRangesMap.get(minKey).get()) {
                    partitionOffsetMap.put(o.partition(), o.untilOffset());
                    //offsetManager.commitOffsetAsync(auth, o.partition(), o.untilOffset());
                }
                OffsetManager.getInstance().commitOffsetAsync(partitionOffsetMap);
                try {
                    OffsetTools.writeOffsetFileToHdfs(offsetFileName, partitionOffsetMap);
                } catch (Exception e) {
                    e.printStackTrace();
                    Tools.printErrLog("write offset failed ！！！");
                    System.exit(-1);
                }
                partitionOffsetMap.clear();
                offsetRangesMap.remove(minKey);

            }
        });
        jssc.start();
        jssc.awaitTermination();
    }

    public static void dealWriteException(String filenameWithPath, StringBuffer dataStringBuffer){
        try {
            Tools.printErrLog(filenameWithPath + " **Failed to append for the first time. Replace the file below!** ");
            Thread.sleep(5000L);
            HDFSTools.newFileAppendRTDataByList(filenameWithPath, dataStringBuffer, 8192);
        } catch (Exception e1) {
            Tools.printErrLog(filenameWithPath + " **Failed to replace the first file. Next, replace the second file! "
                    + e1.getMessage());
            e1.printStackTrace();
            try {
                Thread.sleep(5000L);
                HDFSTools.newFileAppendRTDataByList(filenameWithPath, dataStringBuffer, 8192);
            } catch (Exception e2) {
                Tools.printErrLog(filenameWithPath + "**The second file replacement failed, program exit!! "
                        + e2.getMessage());
                e2.printStackTrace();
                System.exit(-1);
            }
        }
    }

    public JavaInputDStream<ConsumerRecord<String, RtfData>> lanch(JavaStreamingContext jssc,
                                                                   String topicName,
                                                                   String brokerlist,
                                                                   String groupId
    ) {
        List<String> topics = new ArrayList();
        topics.add(topicName);
        Map<String, Object> kafkaParams = KafkaUtil.getKafkaParam(brokerlist,groupId);
        kafkaParams.put("topic.partition.subconcurrency", subconcurrency);
//    kafkaParams.put("auto.commit.enable", "true");
//    kafkaParams.put("auto.commit.interval.ms", String.valueOf(6000));
        JavaInputDStream<ConsumerRecord<String, RtfData>> kafkaStreaming = KafkaUtils
                .createDirectStream(jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, RtfData>Subscribe(topics, kafkaParams));
        return kafkaStreaming;
    }

    public String processData(RtfData data) {
        System.out.println("==============================" + data.toString());
        Map<String, String> rtdataMap;
        StringBuffer result = new StringBuffer();
        long ts = data.getTs();
        String opt = data.getOpt().toString().toLowerCase();
        Map<CharSequence, CharSequence> rtdataCharMap = new HashMap<CharSequence, CharSequence>();
        StringBuffer outKey = new StringBuffer();
        StringBuffer value = new StringBuffer();
        if (DELETE.equals(opt)) {
            opt = Constants.OPT_ENUM.DELETE.getValue();
            rtdataCharMap = data.getSrc();
            rtdataMap = transform(rtdataCharMap);
            for (String index_key : primaryKeyArray) {
                outKey.append(rtdataMap.get(index_key)).append(KEY_SPLIT_U0001);
            }

        } else if (UPDATE.equals(opt) || INSERT.equals(opt)) {
            if (UPDATE.equals(opt)) {
                opt = Constants.OPT_ENUM.UPDATE.getValue();
                rtdataCharMap = data.getSrc();
                for (CharSequence k : data.getCur().keySet()) {
                    rtdataCharMap.put(k, data.getCur().get(k));
                }
            } else if (INSERT.equals(opt)) {
                opt = Constants.OPT_ENUM.INSERT.getValue();
                rtdataCharMap = data.getCur();
            }
            rtdataMap = transform(rtdataCharMap);
            try {
                for (String index_key : primaryKeyArray) {
                    outKey.append(rtdataMap.get(index_key)).append(KEY_SPLIT_U0001);
                }
                for (int i = 0; i < fieldArray.length - 1; i++) {
                    if (null == rtdataMap.get(fieldArray[i])) {
                        value.append(NULL).append(SPLIT_TAB);
                    } else {
                        value.append(cleanData(rtdataMap.get(fieldArray[i]))).append(SPLIT_TAB);
                    }
                }
                if (null == rtdataMap.get(fieldArray[fieldArray.length - 1])) {
                    value.append(NULL);
                } else {
                    value.append(cleanData(rtdataMap.get(fieldArray[fieldArray.length - 1])));
                }
            } catch (Exception e) {
                Tools.printLog("rtdataMap get exception：");
                e.printStackTrace();
            }
        }
        result.append(outKey.substring(0, outKey.length() - 1).trim()).append(SPLIT_TAB)
                .append(ts).append(SPLIT_TAB)
                .append(opt).append(SPLIT_TAB)
                .append(value).append(ENTER);
        return result.toString();
    }

    /**
     * @param srcMap
     * @return key lowercase
     */
    public static Map<String, String> transform(Map<CharSequence, CharSequence> srcMap) {
        if (srcMap == null) {
            return null;
        } else {
            Map<String, String> destMap = new HashMap();
            Iterator it = srcMap.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<CharSequence, CharSequence> e = (Map.Entry) it.next();
                String key = e.getKey() == null ? null : ((CharSequence) e.getKey()).toString();
                key = key.toLowerCase();
                String value = e.getValue() == null ? null : ((CharSequence) e.getValue()).toString();
                destMap.put(key, value);
            }
            return destMap;
        }
    }

    /**
     * @param data
     * @return
     */
    public static String cleanData(String data) {
        if (null == data || Strings.isNullOrEmpty(data)) {
            return data;
        } else {
            return data.replace("\t", "")
                    .replace("\r", "")
                    .replace("\n", "");
        }
    }

    public String findFile(String key) {
        String filename = filePartitionInfoCache.findFilenameByKey(String.valueOf(key.hashCode())
                .replace("-", ""));
        return filename;
    }

}