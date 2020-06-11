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

package com.jd.dw.rtf.writer.his.emptyfile;

import com.jd.dw.rtf.writer.Constants;
import com.jd.dw.rtf.writer.his.partitioner.RTFPartitioner;
import com.jd.dw.rtf.writer.tools.HDFSTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.net.URI;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * java -cp HisInit-2.0.jar com.jd.dw.rtf.writer.his.emptyfile.HistoryInitEmpty hdfs://ns13/user/dd_edw_rtf/fdm_rtf.db/test_fdm_order_rtf1 5
 */
public class HistoryInitEmpty {
  private static Configuration conf ;
  static {
    conf = new Configuration();
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);//解决Filesystem closed
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
    conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
  }


  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.out.println("please input : outputPath,fileNum");
    }
    String outputPath = args[0];
    int fileNum = Integer.parseInt(args[1]);
    System.out.println("outputPath: " + outputPath);
    System.out.println("fileNum:" + fileNum);

    Set<String> filenameSet = getFilenames(fileNum);

    try {
      FileSystem fs = FileSystem.get(URI.create(outputPath), conf);
      HDFSTools.mkdir(outputPath);

      for (String filename : filenameSet) {
        createFile(filename,outputPath);
      }
      fs.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public static Set<String> getFilenames(int fileNum){
    Random random = new Random();

    TreeSet<String> filenameSet = new TreeSet<String>();
    int n = 999999999;
    for (int i = 0; i < fileNum; i++) {
      Text key = new Text(String.valueOf(random.nextInt(n)));
      int partition = RTFPartitioner.calPartition(key, fileNum);
      int startKey = partition * (Constants.PARTITION_FACTOR / fileNum);
      int endKey = (partition + 1) * (Constants.PARTITION_FACTOR / fileNum) - 1;

      StringBuffer fileName = new StringBuffer().append(startKey).append(Constants.FILE_NAME_SPLIT)
              .append(endKey).append(Constants.HIS_FLAG_STR).append(i);
      filenameSet.add(fileName.toString());
    }
    return filenameSet;
  }

  public static void createFile(String filename , String outputPath) throws Exception{
    runCmd("touch " + filename);
    runCmd("lzop " + filename);
    runCmd("mv " + filename+Constants.LZO_SUFFIX +" " +filename+Constants.RTF_SUFFIX);
    runCmd("hadoop fs -put " + filename+Constants.RTF_SUFFIX+" " +outputPath);
    runCmd("rm "+filename);
    runCmd("rm "+filename+Constants.RTF_SUFFIX);
  }

  public static void runCmd(String cmd) throws Exception{
    Process p = Runtime.getRuntime().exec(cmd);
    p.waitFor();

  }

}