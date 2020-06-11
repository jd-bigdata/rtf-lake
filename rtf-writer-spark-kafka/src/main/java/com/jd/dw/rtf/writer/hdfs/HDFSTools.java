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

package com.jd.dw.rtf.writer.hdfs;

import com.jd.dw.rtf.writer.Constants;
import com.jd.dw.rtf.writer.tools.Tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author anjinlong
 * @create 2017-07-10 14:32
 * @description description
 **/
public class HDFSTools {
  private static Configuration conf;
  private static HashMap<String, String> schemaMap = new HashMap<String, String>();
  private static ConcurrentHashMap<Integer, String> fieldmap;

  static {
    conf = new Configuration();
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);//解决Filesystem closed
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
    conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
    conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
  }

  public static boolean isDirExist(String path) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(path), conf);
    boolean flag = fs.exists(new Path(path));
    fs.close();
    return flag;
  }

  public static void createDir(String path) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(path), conf);
    fs.mkdirs(new Path(path));
    fs.close();
  }
  //buffSize 65536  4096
  public static boolean appendRTDataByList(String hdfsFile, StringBuffer dataStringBuffer, int
          buffSize)
          throws Exception {
    boolean flag = false;

    FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);

    InputStream is = new ByteArrayInputStream(dataStringBuffer.toString().getBytes());
    InputStream in = new BufferedInputStream(is);
    FSDataOutputStream out = null;
    try {
      out = fs.append(new Path(hdfsFile));
      IOUtils.copyBytes(in, out, buffSize, true);

    } catch (Exception e) {
      throw e;
    } finally {
      try {
        in.close();
        fs.close();
        if( null != out){
          out.close();
        }
      } catch (Exception e1) {
        Tools.printErrLog("file:" + hdfsFile);
        e1.printStackTrace();
      }
    }

    return flag;
  }

  public static boolean newFileAppendRTDataByList(String hdfsFile, StringBuffer dataStringBuffer,
                                                  int buffSize)
          throws Exception {
    boolean flag = true;
    StringBuffer writeData = new StringBuffer();
    FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
    Path path = new Path(hdfsFile);
    FSDataInputStream fsin = fs.open(path);
    BufferedReader br = null;
    String line;
    int oldcnt = 0;
    try {
      br = new BufferedReader(new InputStreamReader(fsin));
      while ((line = br.readLine()) != null) {
        writeData.append(line).append("\n");
        oldcnt++;
      }
    } finally {
      br.close();
      fsin.close();
    }

    writeData.append(dataStringBuffer);
    byte[] data = writeData.toString().getBytes();

    String tmpFile = hdfsFile.replace(Constants.RTF_FILE_PREFIX, "_" + Constants.RTF_FILE_PREFIX);
    fs = FileSystem.get(URI.create(tmpFile), conf);
    FSDataOutputStream fos = fs.create(new Path(tmpFile));
    fos.write(data, 0, data.length);
    fos.close();
    fs.close();

    HDFSTools.delete(hdfsFile);
    HDFSTools.rename(tmpFile, hdfsFile);

    Tools.printErrLog(hdfsFile + " Number of old file lines replaced by files：" + oldcnt);

    return flag;
  }

  public static boolean appendRTDataByFile(String hdfsFile, String appendFilename) {
    boolean flag = false;
    FileSystem fs = null;
    try {
      fs = FileSystem.get(URI.create(hdfsFile), conf);
      InputStream in = new BufferedInputStream(new FileInputStream(appendFilename));
      OutputStream out = fs.append(new Path(hdfsFile));
      IOUtils.copyBytes(in, out, 4096, true);
      fs.close();
    } catch (IOException e) {
      e.printStackTrace();
    }


    return flag;
  }

  public static List<String> getFileNamesByPath(String hdfsDir){
    System.out.println(hdfsDir);
    List<String> filenameList = new ArrayList<String>();
    conf.set("fs.hdfs.impl",
            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
    );
    conf.set("fs.file.impl",
            org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
    try {
      FileSystem fs = FileSystem.get(URI.create(hdfsDir), conf,"root");
      Path path = new Path(hdfsDir);
      if(fs.exists(path)){
        FileStatus[] fileStatus = fs.listStatus(path);
        for(int i = 0; i < fileStatus.length; i++){
          if(!fileStatus[i].isDirectory()){
            filenameList.add(fileStatus[i].getPath().getName());
          }
        }
      }
      fs.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return filenameList;
  }

  public static long getFileSize(String filename) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    Path path = new Path(filename);
    long len = fs.getLength(path);
    fs.close();
    return len;
  }

  public static long getRTDataSize(String filenameWithPath) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(filenameWithPath), conf);
    Path path = new Path(filenameWithPath);
    long fileSize = fs.getLength(path);

    String[] filenames = filenameWithPath.split(Constants.FILE_PATH_SLASH, -1);
    String filename = filenames[filenames.length - 1];
    long HistorySize = Long.parseLong(filename.split(Constants.FILE_NAME_SPLIT, -1)[2]);
    fs.close();
    return fileSize - HistorySize;
  }

  public static Map<String, String> getFilesUnderFolder(String folderPath, String pattern) throws
          IOException {
    FileSystem fs = FileSystem.get(URI.create(folderPath), conf);
    Map<String, String> paths = new HashMap<String, String>();
    Path path = new Path(folderPath);
    if (fs.exists(path)) {
      FileStatus[] fileStatus = fs.listStatus(path);
      for (int i = 0; i < fileStatus.length; i++) {
        FileStatus fileStatu = fileStatus[i];
        if (!fileStatu.isDir()) {
          Path oneFilePath = fileStatu.getPath();
          if (pattern == null) {
            paths.put(oneFilePath.toString(), String.valueOf(fs.getLength(oneFilePath)));
          } else {
            if (oneFilePath.getName().contains(pattern)) {
              paths.put(oneFilePath.toString(), String.valueOf(fs.getLength(oneFilePath)));
            }
          }
        }
      }
    }
    fs.close();
    return paths;
  }

  public static void read(String filename) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(filename), conf);

    Path path = new Path(filename);
    FSDataInputStream fsin = fs.open(path);
    BufferedReader br = null;
    String line;
    int i = 0;
    try {
      br = new BufferedReader(new InputStreamReader(fsin));
      while ((line = br.readLine()) != null) {
        System.out.println(line);
        i++;
      }
    } finally {
      br.close();
    }
  }

  public static boolean isFileExist(String filename) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    boolean flag = fs.exists(new Path(filename));
    fs.close();
    return flag;
  }

  public static void createFile(String filename) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    fs.create(new Path(filename));
    fs.close();
  }

  public static void createNewFile(String filename) throws IOException {
    if (isFileExist(filename)) {
      delete(filename);
    }
    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    fs.create(new Path(filename));
    fs.close();
  }

  public static void rename(String oldName, String newName) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(oldName), conf);
    fs.rename(new Path(oldName), new Path(newName));
    fs.close();
  }

  public static void delete(String filename) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    fs.delete(new Path(filename));
    fs.close();
  }

  public static void testdelete(String filename) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    fs.delete(new Path(filename));
    fs.close();
  }

  public static void write(String hdfsFile, byte[] data) throws IOException {
    FileSystem fs = FileSystem.get(URI.create(hdfsFile), conf);
    FSDataOutputStream fos = fs.create(new Path(hdfsFile));
    fos.write(data, 0, data.length);
    fos.close();
    fs.close();
  }

  public static HashMap<String, String> loadORCRtMap(List<String> list) {
    HashMap<String, String> orcRtMap = new HashMap<String, String>();
    int allcount = 0;
    int elsecount = 0;
    int ifcount = 0;
    int insertcount = 0;
    int updatecount = 0;
    int deletecount = 0;
    int elseallcount = 0;
    int insertcount1 = 0;
    int updatecount1 = 0;
    int deletecount1 = 0;
    int elseallcount1 = 0;
    for (String orcrtdata : list) {
      String[] values = orcrtdata.split("\t", -1);
      String key = values[values.length - 3];
      if (!key.equals("") && !orcrtdata.equals("") && values.length > 2) {
        allcount++;
        if (orcRtMap.keySet().contains(key)) {
          ifcount++;
          String thisUpdateTS = values[values.length - 2];
          String thisOpt = values[values.length - 1];

          String lastValue = orcRtMap.get(key);
          String lastUpdateTS = lastValue.split("\t", -1)[values.length - 2];
          String lastOpt = lastValue.split("\t", -1)[values.length - 1];
          if (thisOpt.equals(Constants.OPT_ENUM.INSERT.getValue())) {
            insertcount++;
          } else if (thisOpt.equals(Constants.OPT_ENUM.UPDATE.getValue())) {
            updatecount++;
          } else if (thisOpt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
            deletecount++;
          } else {
            elseallcount++;
          }
          if (1 == Tools.compareTS(thisUpdateTS, lastUpdateTS)) {//这次的数据新
            if (thisOpt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
              orcRtMap.remove(key);
            } else if (thisOpt.equals(Constants.OPT_ENUM.UPDATE.getValue())) {
              orcRtMap.put(key, orcrtdata);
            }
          } else {
            if (lastOpt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
              orcRtMap.remove(key);
            } else if (lastOpt.equals(Constants.OPT_ENUM.UPDATE.getValue())) {
            }
          }

        } else {
          elsecount++;
          String thisUpdateTS = values[values.length - 2];
          String thisOpt = values[values.length - 1];
          if (thisOpt.equals(Constants.OPT_ENUM.INSERT.getValue())) {
            insertcount1++;
          } else if (thisOpt.equals(Constants.OPT_ENUM.UPDATE.getValue())) {
            updatecount1++;
          } else if (thisOpt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
            deletecount1++;
          } else {
            elseallcount1++;
          }
          orcRtMap.put(key, orcrtdata);
        }
      }
    }
    System.out.println("orcrtmapsize: " + orcRtMap.size() + "list size: " + list.size() +
            "allcount:" + allcount + "elsecount:" + elsecount + "ifcount:" + ifcount +
            "insertcount:" + insertcount + "deletecount:" + deletecount + "updatecount:" +
            updatecount + "elseallcount:" + elseallcount + "insertcount1:" + insertcount1 +
            "deletecount1:" + deletecount1 + "updatecount1:" + updatecount1 + "elseallcount1:" +
            elseallcount1);
    return orcRtMap;
  }

  public static void readHdfsLzop(String filename) throws IOException {
    System.out.println("[" + new Date() + "] : enter read");
    Configuration conf = new Configuration();
    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodec(new Path(filename));
    System.out.println("codec:" + codec);
    if (null == codec) {
      System.out.println("Cannot find codec for file " + filename);
      return;
    }

    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    Path path = new Path(filename);
    InputStream input = fs.open(path);
    input = codec.createInputStream(input);

    BufferedReader br = null;
    String line;
    try {
      br = new BufferedReader(new InputStreamReader(input));
      while ((line = br.readLine()) != null) {
        System.out.println(line);
      }
    } finally {
      br.close();
    }
    input.close();
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


  public static void copyToLocalFile(String filename, String targetFilename) throws Exception {
    conf.set("fs.hdfs.impl",
            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
    );
    conf.set("fs.file.impl",
            org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
    FileSystem fs = FileSystem.get(URI.create(filename), conf);
    fs.copyToLocalFile(new Path(filename), new Path(targetFilename));

  }


  public static void main(String[] args) throws Exception {
    if ("read".equals(args[0])) {
      readHdfsLzop(args[1]);
    } else if ("write".equals(args[0])) {
      writeHdfsLzop(args[1]);
    }

  }

}