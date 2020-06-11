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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @author anjinlong
 * @create 2018-06-29 13:46
 * @description find n max file
 **/

public class CompactionStrategyTopN {

    private static Configuration conf;

    static {
        conf = new Configuration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);//解决Filesystem closed
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
    }

    private static List<String> topNFileName = new ArrayList<String>();

    public static void main(String[] args) {
        getTopNFileName(args[0], 20);
    }


    public static void getTopNFileName(String hdfsDir, int n) {
        topNFileName.clear();
        List<String> allFileList = new ArrayList<String>();
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsDir), conf);
            Path path = new Path(hdfsDir);
            if (fs.exists(path)) {
                FileStatus[] fileStatus = fs.listStatus(path);
                for (int i = 0; i < fileStatus.length; i++) {
                    FileStatus fileStatu = fileStatus[i];
                    if (!fileStatu.isDir()) {
                        Path oneFilePath = fileStatu.getPath();
                        if (oneFilePath.getName().contains(Constants.RT_DATA_FLAG)) {
                            allFileList.add(fileStatu.getLen() + Constants.COMMA + oneFilePath.getName());
                        }
                    }
                }
                Collections.sort(allFileList, new Comparator<String>() {
                    public int compare(String file1, String file2) {
                        Long id1 = Long.parseLong(file1.split(Constants.COMMA)[0]);
                        Long id2 = Long.parseLong(file2.split(Constants.COMMA)[0]);
                        return id2.compareTo(id1);
                    }
                });
                int max = (allFileList.size() > n) ? n : allFileList.size();
                for (int i = 0; i < max; i++) {
                    String[] tmps = allFileList.get(i).split(Constants.COMMA);
                    long filelength = Long.parseLong(tmps[0]);
                    if (filelength > Constants.START_COMPACTION_FACTOR) {
                        System.out.println(allFileList.get(i));
                        topNFileName.add(tmps[1]);
                    }
                }
            }
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static boolean isNeedCompaction(String filenameWithPath) throws IOException {
        boolean flag = false;
        String filename = filenameWithPath.substring(filenameWithPath.lastIndexOf(Constants.FILE_PATH_SLASH) + 1,
                filenameWithPath.length());
        if (topNFileName.contains(filename)) {
            flag = true;
        }
        return flag;
    }

}