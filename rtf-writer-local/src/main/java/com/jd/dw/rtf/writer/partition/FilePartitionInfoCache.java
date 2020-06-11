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

package com.jd.dw.rtf.writer.partition;

import com.jd.dw.rtf.writer.Constants;
import com.jd.dw.rtf.writer.tools.HDFSTools;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FilePartitionInfoCache {
    private static final Logger logger = LoggerFactory.getLogger(FilePartitionInfoCache.class);
    private String path;
    private volatile Map<String, FilePartition> filePartitionInfoMap;

    public FilePartitionInfoCache(String path) {
        this.path = path;
        this.filePartitionInfoMap = new ConcurrentHashMap();
        List<String> filenameList = HDFSTools.getFileNamesByPath(path);
        for (String filename : filenameList) {
            if ((!filename.startsWith(Constants.UNDERLINE)) && (!StringUtils.isEmpty(filename))) {
                if (filename.contains(Constants.RT_DATA_FLAG)) {
                    this.filePartitionInfoMap.put(filename, new FilePartition(filename));
                } else {
                    String rtfName = new StringBuffer(Constants.RTF_FILE_PREFIX)
                            .append(filename.replace(Constants.HISTORY_DATA_FLAG, Constants.RT_DATA_FLAG)).toString();
                    if (!filenameList.contains(rtfName)) {
                        try {
                            HDFSTools.createFile(path + Constants.FILE_PATH_SLASH + rtfName);
                        } catch (IOException e) {
                            logger.error("create new file fail：" + rtfName);
                            e.printStackTrace();
                        }
                        this.filePartitionInfoMap.put(rtfName, new FilePartition(rtfName));
                    }
                }
            }
        }
    }

    /**
     * Start all RTF files in the folder_ Key and end_ Key, put it in the map, and then use the hashcode of
     * the primary key to determine which file to put
     *
     * @param
     */
    public void init() {

        List<String> filenameList = HDFSTools.getFileNamesByPath(path);
        for (String filename : filenameList) {
            if ((!filename.startsWith(Constants.UNDERLINE)) && (!StringUtils.isEmpty(filename))) {
                if (filename.contains(Constants.RT_DATA_FLAG)) {
                    this.filePartitionInfoMap.put(filename, new FilePartition(filename));
                } else {
                    String rtfName = new StringBuffer(Constants.RTF_FILE_PREFIX)
                            .append(filename.replace(Constants.HISTORY_DATA_FLAG, Constants.RT_DATA_FLAG)).toString();
                    if (!filenameList.contains(rtfName)) {
                        try {
                            HDFSTools.createFile(path + Constants.FILE_PATH_SLASH + rtfName);
                        } catch (IOException e) {
                            logger.error("create new file fail：" + rtfName);
                            e.printStackTrace();
                        }
                        this.filePartitionInfoMap.put(rtfName, new FilePartition(rtfName));
                    }
                }
            }
        }
    }

    //通过主键的hashcode寻找filename
    public String findFilenameByKey(String key) {
        String filename = "";
        int partKey;
        if (key.length() <= Constants.PARTITION_KEY_LENGTH) partKey = Integer.parseInt(key);
        else {
            partKey = Integer.parseInt(key.substring(key.length() - Constants.PARTITION_KEY_SUBLENGTH)
                    + key.substring(0, Constants.PARTITION_KEY_SUBLENGTH));
        }

        for (FilePartition fp : this.filePartitionInfoMap.values()) {
            if (fp.isKeyInFile(partKey)) {
                filename = fp.getFilename();
                break;
            }
        }
        return filename;
    }
}