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

package com.jd.dw.rtf.writer.compaction;

import com.jd.dw.rtf.writer.Constants;
import com.jd.dw.rtf.writer.hdfs.HDFSTools;
import com.jd.dw.rtf.writer.tools.Tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * @author anjinlong
 * @create 2017-11-16 9:57
 * @description merge
 **/
public class CompactionWorker {
    private static final Logger logger = LoggerFactory.getLogger(CompactionWorker.class);
    private static final String LZO_CODEC_CLASS_NAME = "com.hadoop.compression.lzo.LzopCodec";
    private static String MAX_INT = String.valueOf(Integer.MAX_VALUE);
    private static Configuration conf;

    static {
        conf = new Configuration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);//fix Filesystem closed
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
    }

    public static void run_compaction(String filename_withpath, String recently_days) {
        try {
            String rtFilenameWithPath = filename_withpath;
            if ((!recently_days.equals(MAX_INT))) {
                String lastNDay = recently_days;
                String lastNDate = Tools.getDateBefore(Integer.parseInt(lastNDay));
                doCompactionRecent(rtFilenameWithPath, lastNDate);
            } else {
                doCompaction(rtFilenameWithPath);
            }
        } catch (Exception e) {
            logger.error("compaction fail :" + e.getMessage());
            e.printStackTrace();
            //todo  exit(255)
        }
    }

    /**
     * Compact live files into history files
     * 1. Add real-time file to memory (delete type reserved)
     * 2. Read LZO history file
     * 3. Create a new LZO history file and compact it with real-time data
     *
     * @param rtFilenameWithPath realtime file
     * @return
     * @throws Exception
     */
    public static String doCompaction(String rtFilenameWithPath) throws Exception {
        long start = System.currentTimeMillis();
        HashMap<String, String> rtDataMap = new HashMap<String, String>();
        int rtcnt = loadRealtimeData(rtFilenameWithPath, rtDataMap);

        long hiscnt = 0;
        String hisFileNameWithPath = rtFilenameWithPath.replace(Constants.RTF_FILE_PREFIX, Constants.EMPTY_STRING)
                .replace(Constants.RT_DATA_FLAG, Constants.HISTORY_DATA_FLAG);
        logger.info("history file：" + hisFileNameWithPath);
        Class codecClass = Class.forName(LZO_CODEC_CLASS_NAME);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        FileSystem fs = FileSystem.get(URI.create(hisFileNameWithPath), conf);
        Path path = new Path(hisFileNameWithPath);
        InputStream input = fs.open(path);
        input = codec.createInputStream(input);
        long rescnt = 0;
        String tmpHisFileNameWithPath = hisFileNameWithPath.substring(0, hisFileNameWithPath
                .lastIndexOf(Constants.FILE_PATH_SLASH) + 1)
                + Constants.UNDERLINE + hisFileNameWithPath.substring
                (hisFileNameWithPath.lastIndexOf(Constants.FILE_PATH_SLASH) + 1, hisFileNameWithPath.length());
        logger.info("tmp file：" + tmpHisFileNameWithPath);
        FileSystem.get(URI.create(tmpHisFileNameWithPath), conf);
        FSDataOutputStream fos = fs.create(new Path(tmpHisFileNameWithPath));
        CompressionOutputStream cout = codec.createOutputStream(fos);
        HashSet<String> alreadyReadKey = new HashSet<String>();
        List<String> resultList = new LinkedList<String>();
        BufferedReader br = null;
        String oneHisRecord;
        try {
            br = new BufferedReader(new InputStreamReader(input));
            while ((oneHisRecord = br.readLine()) != null) {
                hiscnt++;
                if (hiscnt % 10000 == 0) {
                    logger.info("hiscnt: " + hiscnt);
                    logger.info("rescnt: " + rescnt);
                }
                String[] values = oneHisRecord.split(Constants.SPLIT_TAB, -1);
                String primaryKey = values[0];
                String thisUpdateTS = values[1];
                String thisOpt = values[2];
                if (rtDataMap.containsKey(primaryKey)) {
                    alreadyReadKey.add(primaryKey);
                    String rtValue = rtDataMap.get(primaryKey);
                    String[] rtValues = rtValue.split(Constants.SPLIT_TAB, -1);
                    if (values.length >= 3 && rtValues.length >= 3) {
                        String rtUpdateTS = rtValues[1];
                        String rtOpt = rtValues[2];
                        if (1 == Tools.compareTS(rtUpdateTS, thisUpdateTS)) {
                            if (rtOpt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
                            } else {
                                resultList.add(rtValue);
                            }
                        } else {
                            if (thisOpt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
                            } else {
                                resultList.add(oneHisRecord);
                            }
                        }
                    }
                } else {
                    if (thisOpt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
                    } else {
                        resultList.add(oneHisRecord);
                    }
                }
                if (resultList.size() == 10000) {
                    rescnt += resultList.size();
                    StringBuffer tmpstr = new StringBuffer();
                    for (String line : resultList) {
                        tmpstr.append(line).append(Constants.ENTER);
                    }
                    byte[] data = tmpstr.toString().getBytes();
                    cout.write(data, 0, data.length);
                    resultList.clear();
                }
            }//end while
            rescnt += resultList.size();
            StringBuffer lastTmpstr = new StringBuffer();
            for (String line : resultList) {
                lastTmpstr.append(line).append(Constants.ENTER);
            }
            byte[] data = lastTmpstr.toString().getBytes();
            cout.write(data, 0, data.length);
            resultList.clear();
            if (rtDataMap.size() != alreadyReadKey.size()) {
                for (String k : rtDataMap.keySet()) {
                    if (!alreadyReadKey.contains(k)) {
                        String rtValue = rtDataMap.get(k);
                        String[] values = rtValue.split(Constants.SPLIT_TAB, -1);
                        String opt = values[2];
                        if (opt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
                        } else {
                            resultList.add(rtValue);
                        }
                    }
                }
            }
            rescnt += resultList.size();
            StringBuffer insertTmpstr = new StringBuffer();
            for (String line : resultList) {
                insertTmpstr.append(line).append(Constants.ENTER);
            }
            data = insertTmpstr.toString().getBytes();
            cout.write(data, 0, data.length);
            resultList.clear();
        } finally {
            br.close();
        }
        input.close();
        cout.close();
        fos.close();
        fs.close();
        HDFSTools.delete(hisFileNameWithPath);
        HDFSTools.rename(tmpHisFileNameWithPath, hisFileNameWithPath);
        HDFSTools.createNewFile(rtFilenameWithPath);
        long finish = System.currentTimeMillis();
        logger.info("compaction finish：" + hisFileNameWithPath + ",  duration："
                + (finish - start) + ",历史数据条数：" + hiscnt
                + ",  realtim data count：" + rtcnt + ",  result count：" + rescnt
        );
        return hisFileNameWithPath;
    }

    /**
     * @param rtFilenameWithPath realtime file eg: hdfs://path1/tb1/.rtf.10500000-10624999-rtf-r-00084.rtf
     * @param lastNDate          Keep data from recent days
     * @return
     * @throws Exception
     */
    public static String doCompactionRecent(String rtFilenameWithPath, String lastNDate) throws
            Exception {
        long start = System.currentTimeMillis();
        HashMap<String, String> rtDataMap = new HashMap<String, String>();
        int rtcnt = loadRealtimeData(rtFilenameWithPath, rtDataMap);
        long hiscnt = 0;
        String hisFileNameWithPath = rtFilenameWithPath.replace(Constants.RTF_FILE_PREFIX, "")
                .replace(Constants.RT_DATA_FLAG, Constants.HISTORY_DATA_FLAG);
        logger.info("history file：" + hisFileNameWithPath);
        Class codecClass = Class.forName(LZO_CODEC_CLASS_NAME);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
        FileSystem fs = FileSystem.get(URI.create(hisFileNameWithPath), conf);
        Path path = new Path(hisFileNameWithPath);
        InputStream input = fs.open(path);
        input = codec.createInputStream(input);
        long rescnt = 0;
        String tmpHisFileNameWithPath = hisFileNameWithPath.substring(0, hisFileNameWithPath
                .lastIndexOf(Constants.FILE_PATH_SLASH) + 1)
                + Constants.UNDERLINE + hisFileNameWithPath.substring
                (hisFileNameWithPath.lastIndexOf(Constants.FILE_PATH_SLASH) + 1, hisFileNameWithPath.length());
        logger.info("tmp file：" + tmpHisFileNameWithPath);
        FileSystem.get(URI.create(tmpHisFileNameWithPath), conf);
        FSDataOutputStream fos = fs.create(new Path(tmpHisFileNameWithPath));
        CompressionOutputStream cout = codec.createOutputStream(fos);
        HashSet<String> alreadyReadKey = new HashSet<String>();
        List<String> resultList = new LinkedList<String>();
        BufferedReader br = null;
        String oneHisRecord;
        try {
            br = new BufferedReader(new InputStreamReader(input));
            while ((oneHisRecord = br.readLine()) != null) {
                hiscnt++;
                if (hiscnt % 10000 == 0) {
                    logger.info("hiscnt: " + hiscnt);
                }
                String[] values = oneHisRecord.split(Constants.SPLIT_TAB, -1);
                String primaryKey = values[0];
                String thisUpdateTS = values[1];
                String thisOpt = values[2];
                if (1 == Tools.compareTS(thisUpdateTS, lastNDate)) {//Data in n days
                    if (rtDataMap.containsKey(primaryKey)) {//new data
                        alreadyReadKey.add(primaryKey);
                        String rtValue = rtDataMap.get(primaryKey);
                        String[] rtValues = rtValue.split(Constants.SPLIT_TAB);
                        if (values.length >= 3 && rtValues.length >= 3) {
                            String rtUpdateTS = rtValues[1];
                            String rtOpt = rtValues[2];

                            if (1 == Tools.compareTS(rtUpdateTS, thisUpdateTS)) {//Real time data is NEW
                                if (rtOpt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
                                } else {
                                    resultList.add(rtValue);
                                }
                            } else {//historical data is new
                                if (thisOpt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
                                } else {
                                    resultList.add(oneHisRecord);
                                }
                            }
                        }
                    } else {//no more new data
                        if (thisOpt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
                        } else {
                            resultList.add(oneHisRecord);
                        }
                    }
                }
                if (resultList.size() == 10000) {
                    rescnt += resultList.size();
                    StringBuffer tmpstr = new StringBuffer();
                    for (String line : resultList) {
                        tmpstr.append(line).append(Constants.ENTER);
                    }
                    byte[] data = tmpstr.toString().getBytes();
                    cout.write(data, 0, data.length);
                    resultList.clear();
                }
            }//end while

            //Write less than one batch of data
            rescnt += resultList.size();
            StringBuffer tmpstr = new StringBuffer();
            for (String line : resultList) {
                tmpstr.append(line).append(Constants.ENTER);
            }
            byte[] data = tmpstr.toString().getBytes();
            cout.write(data, 0, data.length);
            resultList.clear();

            //Output data in real-time data that is not compared with old data
            if (rtDataMap.size() != alreadyReadKey.size()) {
                for (String k : rtDataMap.keySet()) {
                    if (!alreadyReadKey.contains(k)) {
                        String rtValue = rtDataMap.get(k);
                        String[] values = rtValue.split(Constants.SPLIT_TAB, -1);
                        String opt = values[2];
                        if (opt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
                        } else {
                            resultList.add(rtValue);
                        }
                    }
                }
            }
            rescnt += resultList.size();
            tmpstr = new StringBuffer();
            for (String line : resultList) {
                tmpstr.append(line).append(Constants.ENTER);
            }
            data = tmpstr.toString().getBytes();
            cout.write(data, 0, data.length);
            resultList.clear();

        } finally {
            br.close();
        }
        input.close();
        cout.close();
        fos.close();
        fs.close();
        try {
            HDFSTools.delete(hisFileNameWithPath);
            HDFSTools.rename(tmpHisFileNameWithPath, hisFileNameWithPath);
        } catch (Exception e) {
            e.printStackTrace();
            HDFSTools.rename(tmpHisFileNameWithPath, hisFileNameWithPath);
            throw e;
        }
        HDFSTools.createNewFile(rtFilenameWithPath);
        long finish = System.currentTimeMillis();
        logger.info("compaction finish：" + hisFileNameWithPath + ",  duration："
                + (finish - start) + ",历史数据条数：" + hiscnt
                + ",  realtim data count：" + rtcnt + ",  result count：" + rescnt
        );
        return hisFileNameWithPath;
    }

    private static int loadRealtimeData(String filename, HashMap<String, String> rtDataMap) throws
            Exception {
        FileSystem fs = FileSystem.get(URI.create(filename), conf);
        Path path = new Path(filename);
        FSDataInputStream fsin = fs.open(path);
        BufferedReader br = null;
        String line;
        int cnt = 0;
        try {
            br = new BufferedReader(new InputStreamReader(fsin));
            while ((line = br.readLine()) != null) {
                put2realTimeDataMap(rtDataMap, line);
                cnt++;
            }
        } finally {
            fs.close();
        }
        return cnt;
    }

    private static void put2realTimeDataMap(HashMap<String, String> realTimeDataMap, String value)
            throws ParseException {
        String[] values = value.split(Constants.SPLIT_TAB, -1);
        String key = values[0];

        if (!key.equals("") && !value.equals("") && values.length > 2) {
            if (realTimeDataMap.keySet().contains(key)) {
                String thisUpdateTS = values[1];
                String lastValue = realTimeDataMap.get(key);
                String lastUpdateTS = lastValue.split(Constants.SPLIT_TAB)[1];
                if (1 == Tools.compareTS(thisUpdateTS, lastUpdateTS)) {
                    realTimeDataMap.put(key, value);
                } else {
                }
            } else {
                realTimeDataMap.put(key, value);
            }
        }
    }
}