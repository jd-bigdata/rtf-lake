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

import com.jd.dw.rtf.writer.compaction.CompactionStrategyTopN;
import com.jd.dw.rtf.writer.compaction.CompactionWorker;
import com.jd.dw.rtf.writer.tools.HDFSTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

public class AppendTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(AppendTask.class);
    private ThreadPoolExecutor executor;
    private CountDownLatch latch;
    private String filenameWithPath;
    private StringBuffer dataStringBuffer;
    private ConcurrentHashMap<String, String> compactionCountMap;
    int i;
    String recentlyNDays;

    public AppendTask(ThreadPoolExecutor executor, CountDownLatch latch, String filenameWithPath,
                      StringBuffer dataStringBuffer, ConcurrentHashMap<String, String>
                              compactionCountMap, int i, String
                              recentlyNDays) {
        this.executor = executor;
        this.latch = latch;
        this.filenameWithPath = filenameWithPath;
        this.dataStringBuffer = dataStringBuffer;
        this.compactionCountMap = compactionCountMap;
        this.i = i;
        this.recentlyNDays = recentlyNDays;
    }

    @Override
    public void run() {
        if (this.dataStringBuffer.length() > 0) {
            long start_append = System.currentTimeMillis();
            try {
                HDFSTools.appendRTDataByList(filenameWithPath, dataStringBuffer, 8192);
            } catch (Exception e) {
                try {
                    logger.error(filenameWithPath + " **Failed to append for the first time. Replace the file below** ");
                    Thread.sleep(5000L);
                    HDFSTools.newFileAppendRTDataByList(filenameWithPath, dataStringBuffer, 8192);
                } catch (Exception e1) {
                    logger.error(filenameWithPath + " **Failed to replace the first file. Next, replace the second file! "
                            + e1.getMessage());
                    e1.printStackTrace();
                    try {
                        Thread.sleep(5000L);
                        HDFSTools.newFileAppendRTDataByList(filenameWithPath, dataStringBuffer, 8192);
                    } catch (Exception e2) {
                        logger.error(filenameWithPath + "**The second file replacement failed, program exit!!"
                                + e2.getMessage());
                        e2.printStackTrace();
                        System.exit(-1);
                    }
                }
            }
            long finish_append = System.currentTimeMillis();
            if ((finish_append - start_append) > 10000) {
                logger.error("num：" + i + " ,latch:" + latch.getCount() + ", " + filenameWithPath +
                        " " +
                        "duration:" +
                        (finish_append - start_append) + " ,count:" +
                        " , ActiveCount:" + executor.getActiveCount() + " , PoolSize:" + executor
                        .getPoolSize() + " ,queue.size: " + executor.getQueue().size());
            }
        }
        try {
            if (CompactionStrategyTopN.isNeedCompaction(filenameWithPath)) {
                CompactionWorker.run_compaction(filenameWithPath, recentlyNDays);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        latch.countDown();
    }
}