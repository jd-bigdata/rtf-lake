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

import com.google.common.base.Strings;
import com.jd.dw.rtf.writer.Constants;
import com.jd.dw.rtf.writer.hdfs.HDFSTools;
import com.jd.dw.rtf.writer.tools.Tools;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FilePartitionInfoCacheBinary implements Serializable {

  private volatile Map<String, FilePartition> filePartitionInfoMap;
  private volatile ArrayList<Long> sortedFilenameList = new ArrayList<Long>();
  private volatile Map<Long, String> sortedFilenameMap = new HashMap<Long, String>();

  public FilePartitionInfoCacheBinary(String path) {
    long start = System.currentTimeMillis();
    this.filePartitionInfoMap = new ConcurrentHashMap();
    List<String> filenameList = HDFSTools.getFileNamesByPath(path);
    for (String filename : filenameList) {
      if ((!filename.startsWith("_")) && (!Strings.isNullOrEmpty(filename))) {
        if (filename.contains("-rtf-")) {
          this.filePartitionInfoMap.put(filename, new FilePartition(filename));

          String name = filename.replace(Constants.RTF_FILE_PREFIX, "");
          String[] filenames = name.split(Constants.FILE_NAME_SPLIT);
          Long startKey = Long.parseLong(filenames[0]);
          sortedFilenameList.add(startKey);
          sortedFilenameMap.put(startKey, filename);
        } else {
          String rtfName = new StringBuffer(Constants.RTF_FILE_PREFIX)
                  .append(filename.replace("-his-", "-rtf-")).toString();
          if (!filenameList.contains(rtfName)) {
            try {
              HDFSTools.createFile(path + "/" + rtfName);
            } catch (IOException e) {
              e.printStackTrace();
            }
            this.filePartitionInfoMap.put(rtfName, new FilePartition(rtfName));

            String name = rtfName.replace(Constants.RTF_FILE_PREFIX, "");
            String[] filenames = name.split(Constants.FILE_NAME_SPLIT);
            Long startKey = Long.parseLong(filenames[0]);
            sortedFilenameList.add(startKey);
            sortedFilenameMap.put(startKey, rtfName);
          }
        }
      }
    }

    Collections.sort(sortedFilenameList);

    Tools.printLog("sortedFilenameList size:" + sortedFilenameList.size() + "," +
            "sortedFilenameList:" + sortedFilenameList.toString());
    Tools.printLog("sortedFilenameMap size:" + sortedFilenameMap.size());


    long finish = System.currentTimeMillis();
    Tools.printLog("FilePartitionInfoCache size：" + filePartitionInfoMap.size() + " duration：" + (finish
            - start));

  }

  public void init(String path) {
    this.filePartitionInfoMap = new ConcurrentHashMap();
    List<String> filenameList = HDFSTools.getFileNamesByPath(path);
    for (String filename : filenameList) {
      if ((!filename.startsWith("_")) && (!Strings.isNullOrEmpty(filename))) {
        if (filename.contains("-rtf-")) {
          this.filePartitionInfoMap.put(filename, new FilePartition(filename));
        } else {
          String rtfName = new StringBuffer(Constants.RTF_FILE_PREFIX)
                  .append(filename.replace("-his-", "-rtf-")).toString();
          if (!filenameList.contains(rtfName)) {
            try {
              HDFSTools.createFile(path + "/" + rtfName);
            } catch (IOException e) {
              Tools.printErrLog("new file exception：" + rtfName);
              e.printStackTrace();
            }
            this.filePartitionInfoMap.put(rtfName, new FilePartition(rtfName));
          }
        }
      }
    }
  }

  public String findFilenameByKey(String key) {
    long partKey;
    if (key.length() <= Constants.PARTITION_KEY_LENGTH) partKey = Integer.parseInt(key);
    else {
      partKey = Integer.parseInt(key.substring(key.length() - Constants.PARTITION_KEY_SUBLENGTH)
              + key.substring(0, Constants.PARTITION_KEY_SUBLENGTH));
    }

    int index = binarySearch(sortedFilenameList, partKey);
    if (index == -999) {
      return null;
    } else {
      return sortedFilenameMap.get(sortedFilenameList.get(index));
    }

  }


  public void remove(String filename) {
    this.filePartitionInfoMap.remove(filename);
  }

  public void add(String filename) {
    this.filePartitionInfoMap.put(filename, new FilePartition(filename));
  }

  public int binarySearch(List list, Long key) {
    int low = 0;
    int high = list.size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      Long midVal = (Long) list.get(mid);
      int cmp = midVal.compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
        if (low < list.size()) {
          Long lowVal = (Long) list.get(low);
          if (lowVal.compareTo(key) > 0) {
            return low;
          }
        }
      } else if (cmp > 0) {
        high = mid - 1;
        if (high >= 0) {
          Long highVal = (Long) list.get(high);
          if (highVal.compareTo(key) < 0) {
            return high;
          }
        }
      } else {
        return mid; // key found
      }
    }
    return -999;  // key not found
  }

}