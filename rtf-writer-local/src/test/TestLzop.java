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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;

/**
 * @author anjinlong
 * @create 2017-11-14 9:32
 * @description description
 **/
public class TestLzop {
  private static Configuration conf;
  private static Class codecClass;
  private static CompressionCodec codec;

  static {
    conf = new Configuration();
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);//解决Filesystem closed
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
    conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
    try {
      codecClass = Class.forName("com.hadoop.compression.lzo.LzopCodec");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
  }

  public static void main(String[] args) {
    try {
//      readHdfs("hdfs://ns6/user/dd_edw_test/fdm_test" +
//              ".db/fdm_pek_orders_chain_rtf_test/lzo/99000000-99999999-r-00099.lzo");
//"078724_0.lzo"
      decompres(args[0]);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void decompres(String filename) throws FileNotFoundException, IOException {
    System.out.println("[" + new Date() + "] : enter compress");

    Configuration conf = new Configuration();
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(new Path(filename));
    if (null == codec) {
      System.out.println("Cannot find codec for file " + filename);
      return;
    }

    File fout = new File(filename + ".decoded");
    InputStream cin = codec.createInputStream(new FileInputStream(filename));
    OutputStream out = new FileOutputStream(fout);

    System.out.println("[" + new Date() + "]: start decompressing ");
    IOUtils.copyBytes(cin, out, 1024 * 1024 * 5, false);
    System.out.println("[" + new Date() + "]: decompressing finished ");

    cin.close();
    out.close();
  }

  public static boolean isFileExist(String filename) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    boolean flag = fs.exists(new Path(filename));
    fs.close();
    return flag;
  }

  public static void readHdfs(String filename) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(filename), conf);

    Path path = new Path(filename);
    InputStream input = fs.open(path);
    input = codec.createInputStream(input);

    BufferedReader br = null;
    String line;
    int i = 0;
    try {
      br = new BufferedReader(new InputStreamReader(input));
      while ((line = br.readLine()) != null) {
        System.out.println(line);
        i++;
      }
    } finally {
      br.close();
    }
  }


}