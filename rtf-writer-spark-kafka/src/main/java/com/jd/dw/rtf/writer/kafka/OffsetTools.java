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

package com.jd.dw.rtf.writer.kafka;

import com.jd.dw.rtf.writer.hdfs.HDFSTools;
import com.jd.dw.rtf.writer.tools.Tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @author anjinlong
 * @create 2018-03-15 16:32
 * @description description
 **/
public class OffsetTools {

  private static Configuration conf;

  static {
    conf = new Configuration();
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);//解决Filesystem closed
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
    conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
  }

  public static Map<Integer, Long> readOffsetFile(String filename) {
    Map<Integer, Long> datamap = new HashMap<Integer, Long>();
    File file = new File(filename);
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(file));
      String tmpString = null;
      while ((tmpString = reader.readLine()) != null) {
        Tools.printLog(tmpString);
        String[] tmps = tmpString.split("=");
        datamap.put(Integer.parseInt(tmps[0]), Long.parseLong(tmps[1]));
      }
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e1) {
        }
      }
    }
    return datamap;
  }

  public static Map<Integer, Long> readOffsetFileFromHdfs(String filename) throws Exception {
    Map<Integer, Long> datamap = new HashMap<Integer, Long>();

    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    Path path = new Path(filename);
    if(!HDFSTools.isFileExist(filename)){
      Tools.printLog("offset msg is not exist!");
      return datamap;
    }

    FSDataInputStream fsin = fs.open(path);
    BufferedReader br = null;
    String tmpString;
    try {
      br = new BufferedReader(new InputStreamReader(fsin));
      while ((tmpString = br.readLine()) != null) {
        Tools.printLog(tmpString);
        String[] tmps = tmpString.split("=");
        datamap.put(Integer.parseInt(tmps[0]), Long.parseLong(tmps[1]));
      }
    } finally {
      br.close();
    }

    return datamap;
  }

  public static boolean writeOffsetFile(String filename, Map<Integer, Long> dataMap) {
    boolean flag = true;
    FileWriter fw = null;
    try {
      fw = new FileWriter(filename);

      for (int part : dataMap.keySet()) {
        Tools.printLog(part + " : " + dataMap.get(part));
        fw.write(part + "=" + dataMap.get(part) + "\r\n");
      }

      fw.close();
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        fw.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return flag;
  }


  public static void writeOffsetFileToHdfs(String filename, Map<Integer, Long> dataMap) throws
          Exception {
    StringBuffer offsets = new StringBuffer();
    for (int part : dataMap.keySet()) {
      Tools.printLog(part + " : " + dataMap.get(part));
      offsets.append(part + "=" + dataMap.get(part) + "\r\n");
    }

    byte[] data = offsets.toString().getBytes();

    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    HDFSTools.createNewFile(filename);

    FSDataOutputStream fos = fs.create(new Path(filename));
    fos.write(data, 0, data.length);
    fos.close();
    fs.close();
  }


}