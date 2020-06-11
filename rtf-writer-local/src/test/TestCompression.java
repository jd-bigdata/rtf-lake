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

package com.jd.dw.rtf.writer.Testing;

import com.jd.dw.rtf.writer.tools.Tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

/**
 * @author anjinlong
 * @create 2018-04-03 11:41
 * @description description java -cp RTFWriter-1.0-SNAPSHOT.jar com.jd.dw.rtf.writer.Testing.TestCompression hdfs://ns13/user/dd_edw_rtf/fdm_rtf.db/test_rtf_fdm_pek_orders/99950000-99999999-his-r-01999.rtf
 **/
public class TestCompression {

  private static Configuration conf;

  static {
    conf = new Configuration();
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);//解决Filesystem closed
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
    conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
  }

  public static void main(String[] args) throws Exception {
    testReadCompression(args[0]);
  }

  public static void testReadCompression(String fileNameWithPath) throws Exception {
    Class codecClass = Class.forName("com.jd.dw.rtf.writer.compression.LzopCodec");
    CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

    FileSystem fs = FileSystem.get(URI.create(fileNameWithPath), conf);
    Path path = new Path(fileNameWithPath);
    InputStream input = fs.open(path);
    input = codec.createInputStream(input);

    BufferedReader br = null;
    String oneHisRecord;
    int hiscnt = 0;
    try {
      br = new BufferedReader(new InputStreamReader(input));
      while ((oneHisRecord = br.readLine()) != null) {
        hiscnt++;
        if (hiscnt % 10000 == 0) {
          Tools.printLog("hiscnt: " + hiscnt);
          String[] values = oneHisRecord.split("\t", -1);
          Tools.printLog(oneHisRecord);
          Tools.printLog(values[0]);
          Tools.printLog(values[0]);
          Tools.printLog("-----");
        }

      }
    } catch (Exception e) {

    }

  }


}