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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.Date;


public class TestCodec {
  private static Configuration conf;

  static {
    conf = new Configuration();
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);//解决Filesystem closed
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
    conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
  }


//  static {
//    try {
//      Class c = com.hadoop.compression.lzo.LzopCodec.class;
//      URL location =
//              c.getProtectionDomain().getCodeSource().getLocation();
//      ZipFile zf = new ZipFile(location.getPath());
//      // liblzo2.so is put in the lib folder
//
//      InputStream in = zf.getInputStream(zf.getEntry("liblzo2.so"));
//      File f = File.createTempFile("JNI-", "Temp");
//      FileOutputStream out = new FileOutputStream(f);
//      byte[] buf = new byte[1024];
//      int len;
//      while ((len = in.read(buf)) > 0)
//        out.write(buf, 0, len);
//      in.close();
//      out.close();
//      System.load(f.getAbsolutePath());
//      f.delete();
//    } catch (Exception e) { // I am still lazy ~~~
//      e.printStackTrace();
//    }
//  }


  public static void main(String[] args) throws Exception {
    if (args[0].equals("compress")) {
      compress(args[1], "org.apache.hadoop.io.compress." + args[2]);
    } else if (args[0].equals("decompress")) {
      decompres(args[1]);
    } else if (args[0].equals("read")) {
      readHdfsLzop(args[1]);
    } else if (args[0].equals("write")) {
      writeHdfsLzop(args[1]);
    } else {
      System.err.println("Error!\n usgae: hadoop jar Hello.jar [compress] [filename] [compress " +
              "type]");
      System.err.println("\t\ror [decompress] [filename] ");
      return;
    }
    System.out.println("down");
  }

  /*
   * filename是希望压缩的原始文件,method是欲使用的压缩方法（如BZip2Codec等）
   */
  public static void compress(String filername, String method) throws ClassNotFoundException,
          IOException {
    System.out.println("[" + new Date() + "] : enter compress");
    File fileIn = new File(filername);
    InputStream in = new FileInputStream(fileIn);

    Class codecClass = Class.forName(method);
    Configuration conf = new Configuration();

    // 通过名称找到对应的编码/解码器
    CompressionCodec codec = (CompressionCodec)
            ReflectionUtils.newInstance(codecClass, conf);

    // 该压缩方法对应的文件扩展名
    File fileOut = new File(filername + codec.getDefaultExtension());
    fileOut.delete();

    OutputStream out = new FileOutputStream(fileOut);
    CompressionOutputStream cout = codec.createOutputStream(out);

    System.out.println("[" + new Date() + "]: start compressing ");
    IOUtils.copyBytes(in, cout, 1024 * 1024 * 5, false);        // 缓冲区设为5MB
    System.out.println("[" + new Date() + "]: compressing finished ");

    in.close();
    cout.close();
  }

  public static void writeHdfsLzop(String hdfsFile) throws Exception {
    Class codecClass = Class.forName("com.hadoop.compression.lzo.LzopCodec");
    Configuration conf = new Configuration();

    CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

    FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
    FSDataOutputStream fos = fs.create(new Path(hdfsFile));
    CompressionOutputStream cout = codec.createOutputStream(fos);

    byte[] data = "aa123\nb1\nc".getBytes();
    cout.write(data, 0, data.length);
    cout.close();
    fos.close();
    fs.close();
  }

  /*
   * filename是希望解压的文件
   */
  public static void decompres(String filename) throws FileNotFoundException, IOException {
    System.out.println("[" + new Date() + "] : enter compress");
    Configuration conf = new Configuration();
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(new Path(filename));
    System.out.println("codec:" + codec);
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

  public static void readHdfsLzop1(String filename) throws Exception {
    System.out.println("[" + new Date() + "] : enter read");
    System.out.println("filename: " + filename);

    Class codecClass = Class.forName("com.hadoop.compression.lzo.LzopCodec");
    CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    Path path = new Path(filename);
    InputStream input = fs.open(path);
    input = codec.createInputStream(input);
    File fout = new File("1.decoded");
    OutputStream out = new FileOutputStream(fout);

    System.out.println("[" + new Date() + "]: start decompressing ");
    IOUtils.copyBytes(input, out, 1024 * 1024 * 5, false);
    System.out.println("[" + new Date() + "]: decompressing finished ");

    input.close();
    out.close();

    File file = new File("1.decoded");
    BufferedReader br = new BufferedReader(new FileReader(file));
    String line;
    int cnt = 0;
    try {
      while ((line = br.readLine()) != null) {
        cnt++;
        if (cnt % 100000 == 0) {
          System.out.println(cnt);
          System.out.println(line);
        }
      }
    } finally {
      br.close();
    }
  }

  public static void readHdfsLzop(String filename) throws Exception {
    System.out.println("[" + new Date() + "] : enter read");
    System.out.println("filename: " + filename);
//    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
//    CompressionCodec codec = factory.getCodec(new Path(filename));

//    System.load("/usr/local/lib/liblzo2.so");
//    System.load("/usr/local/lib/liblzo2.so.2");
//    System.load("/software/servers/hadoop-2.7.1/lib/native/libhadoop.so");
//    System.load("/software/servers/hadoop-2.7.1/lib/native/libgplcompression.so");
//
//    System.setProperty("java.library.path", "/usr/local/lib/");
//    System.loadLibrary("liblzo2.so");
////    System.loadLibrary("/software/servers/hadoop-2.7.1/lib/native/libgplcompression.so");

    Class codecClass = Class.forName("com.hadoop.compression.lzo.LzopCodec");
    CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    Path path = new Path(filename);
    InputStream input = fs.open(path);
    input = codec.createInputStream(input);

    BufferedReader br = null;
    String line;
    int cnt = 0;
    try {
      br = new BufferedReader(new InputStreamReader(input));
      while ((line = br.readLine()) != null) {
        cnt++;
        if (cnt % 10000 == 0) {
          System.out.println(cnt);
        }
      }
    } finally {
      br.close();
    }
    input.close();

    System.out.println("cnt:" + cnt);

  }


}