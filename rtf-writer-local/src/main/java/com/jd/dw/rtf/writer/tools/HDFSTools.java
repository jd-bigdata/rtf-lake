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

import com.jd.dw.rtf.writer.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

;

/**
 * @author anjinlong
 * @create 2017-07-10 14:32
 * @description description
 **/
public class HDFSTools {
    private static final Logger logger = LoggerFactory.getLogger(HDFSTools.class);
    private static Configuration conf;
    private static HashMap<String, String> schemaMap = new HashMap<String, String>();
    private static ConcurrentHashMap<Integer, String> fieldmap;
    private static HashMap<String, String> orcRtMap;

    static {
        conf = new Configuration();
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
    }

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
                out.close();
            } catch (Exception e1) {
                logger.error("file:" + hdfsFile + " close exception：" + e1.getMessage());
            }
        }
        return flag;
    }

    /**
     * @param hdfsFile
     * @param dataStringBuffer
     * @param buffSize
     * @return
     * @throws Exception
     */
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
        String tmpFile = hdfsFile.replace(Constants.RTF_FILE_PREFIX
                , Constants.UNDERLINE + Constants.RTF_FILE_PREFIX);
        writeBytesInFile(tmpFile, data, conf);
        HDFSTools.delete(hdfsFile);
        HDFSTools.rename(tmpFile, hdfsFile);
        logger.error(hdfsFile + " file replace oldcnt：" + oldcnt);
        return flag;
    }


    //chen
    public static List<String> getFileNamesByPath(String hdfsDir) {
        List<String> filenameList = new ArrayList<String>();
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        try {
            FileSystem fs = FileSystem.get(URI.create(hdfsDir), conf, "root");
            Path path = new Path(hdfsDir);
            if (fs.exists(path)) {
                FileStatus[] fileStatus = fs.listStatus(path);
                for (int i = 0; i < fileStatus.length; i++) {
                    if (!fileStatus[i].isDirectory()) {
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
        fs.delete(new Path(filename), true);
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

    public static void writeBytesInFile(String filename, byte[] bytes, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(filename), conf);
        FSDataOutputStream fsOut = fs.create(new Path(filename));
        fsOut.write(bytes, 0, bytes.length);
        fs.close();
        fsOut.close();
    }
}