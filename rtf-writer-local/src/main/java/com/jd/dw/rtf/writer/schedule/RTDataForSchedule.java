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

package com.jd.dw.rtf.writer.schedule;

import com.jd.dw.rtf.writer.Constants;
import com.jd.dw.rtf.writer.compaction.CompactionStrategyTopN;
import com.jd.dw.rtf.writer.partition.FilePartitionInfoCache;
import com.jd.dw.rtf.writer.tools.LogTools;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.concurrent.*;

public class RTDataForSchedule {
    private static final Logger logger = LoggerFactory.getLogger(RTDataForSchedule.class);
    private static ConcurrentHashMap<String, String> compactionCountMap;
    private static Configuration conf;
    private static ThreadPoolExecutor executor;

    static {
        conf = new Configuration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
        compactionCountMap = new ConcurrentHashMap();

        executor = new ThreadPoolExecutor(Constants.CORE_POOL_SIZE,
                Constants.MAX_POOL_SIZE, 200L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue
                (2000), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private String hdfsDir;
    private volatile FilePartitionInfoCache filePartitionInfoCache;
    private HashMap<String, StringBuffer> rtDataMap;
    private int count = 0;
    private int errCount = 0;
    private long startConsumTs = System.currentTimeMillis();


    public RTDataForSchedule(String hdfsDir) throws IOException {
        this.hdfsDir = hdfsDir;
        this.filePartitionInfoCache = new FilePartitionInfoCache(hdfsDir);
        this.rtDataMap = new HashMap();
    }

    public void processRTData(String key,
                              long ts,
                              String opt,
                              String value,
                              String recentlyNDays) throws ParseException {
        StringBuffer result = new StringBuffer().append(key.trim()).append(Constants.SPLIT_TAB)
                .append(ts).append(Constants.SPLIT_TAB).append(opt).append(Constants.SPLIT_TAB)
                .append(value).append(Constants.ENTER);

        try {
            String filename = filePartitionInfoCache.findFilenameByKey(String.valueOf(key.hashCode())
                    .replace(Constants.STRIKETHROUGH, Constants.EMPTY_STRING));
            if (rtDataMap.keySet().contains(filename)) {
                rtDataMap.get(filename).append(result);
            } else {
                rtDataMap.put(filename, new StringBuffer().append(result));
            }
        } catch (Exception e) {
            e.printStackTrace();
            errCount += 1;
            logger.error("exception: " + result.toString());
        }
        count += 1;
        if (System.currentTimeMillis() - startConsumTs > Constants.CONSUME_TIME) {
            long startWrite = System.currentTimeMillis();
            CompactionStrategyTopN.getTopNFileName(hdfsDir, Constants.COMPACTION_NUM);
            CountDownLatch latch = new CountDownLatch(rtDataMap.keySet().size());
            int i = 0;
            for (String f : this.rtDataMap.keySet()) {
                i++;
                if (!StringUtils.isEmpty(f)) {
                    AppendTask appendTask = new AppendTask(executor, latch, hdfsDir + "/" + f,
                            rtDataMap.get(f), compactionCountMap, i, recentlyNDays);
                    executor.execute(appendTask);
                } else {
                    latch.countDown();
                }
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long finishWrite = System.currentTimeMillis();
            StringBuffer compactionFileNames = new StringBuffer();
            Enumeration e = compactionCountMap.keys();
            while (e.hasMoreElements()) {
                compactionFileNames.append(e.nextElement()).append(Constants.COMMA);
            }
            logger.error("[writer]writeRTData count: " + count + " errCount:" + errCount + ",duration:" +
                    (finishWrite - startWrite) / 1000 + " thread num：" + executor.getActiveCount() +
                    " finish time：" + finishWrite);
            count = 0;
            errCount = 0;
            rtDataMap.clear();
            rtDataMap = new HashMap();
            filePartitionInfoCache.init();
            compactionCountMap.clear();
            startConsumTs = System.currentTimeMillis();
        }
    }
}