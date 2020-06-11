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

package com.jd.dw.rtf.writer.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TopNFileManager {

  final static String RT_DATA_FLAG = "-rtf-";

  final static String COMMA = ",";

  public static Configuration conf = new Configuration();

  static {
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
    conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
  }


  public static List<String> getTopNFileNameList(String targetHdfsDir, int maxCompactionNum, long startCompactionFactor) {
    List<String> topNFileNameList = new ArrayList<String>();
    List<String> allFileList = new ArrayList<String>();
    try {
      FileSystem fs = FileSystem.get(URI.create(targetHdfsDir), conf);

      Path path = new Path(targetHdfsDir);
      if (fs.exists(path)) {
        FileStatus[] fileStatus = fs.listStatus(path);
        for (int i = 0; i < fileStatus.length; i++) {
          FileStatus fileStatu = fileStatus[i];
          if (!fileStatu.isDir() ) {
            Path oneFilePath = fileStatu.getPath();
            if (oneFilePath.getName().contains(RT_DATA_FLAG)) {
              allFileList.add(fileStatu.getLen() + COMMA + oneFilePath.getName());
            }
          }
        }
        Collections.sort(allFileList, new Comparator<String>() {
          public int compare(String file1, String file2) {
            Long id1 = Long.parseLong(file1.split(COMMA)[0]);
            Long id2 = Long.parseLong(file2.split(COMMA)[0]);
            return id2.compareTo(id1);
          }
        });

        for (int i = 0; i < maxCompactionNum; i++) {
          String[] tmps = allFileList.get(i).split(COMMA);
          long filelength = Long.parseLong(tmps[0]);
          if (filelength > startCompactionFactor) {
            topNFileNameList.add(tmps[1]);
          }
        }

      }
      fs.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return topNFileNameList;
  }


}