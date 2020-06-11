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

package com.jd.dw;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;

public class RTFRecordReader implements RecordReader<LongWritable, Text> {
    private static Configuration conf;
    private static final Log LOG = LogFactory.getLog(RTFRecordReader.class.getName());
    private static final String LZO_CODEC_CLASS = "com.hadoop.compression.lzo.LzopCodec";

    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private RTFRecordReader.LineReader in;
    int maxLineLength;
    private Seekable filePosition;
    private CompressionCodec codec;
    private Decompressor decompressor;
    Path file;
    private long rtDataSize = 0;
    private long rtDataPos = 0;
    private long totalCnt = 0;
    private HashMap<String, String> realTimeDataMap = new HashMap<String, String>();
    int cnt = 0;
    HashSet<String> alreadyReadKey = new HashSet<String>();

    public void initConf(Configuration job) {
        conf = job;
        conf.setBoolean("fs.hdfs.impl.disable.cache", true);//解决Filesystem closed
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        conf.addResource(new Path(System.getenv("HADOOP_CONF_DIR") + "/hdfs-site.xml"));
    }

    public RTFRecordReader(Configuration job, FileSplit split) throws Exception {
        initConf(job);
        LOG.info("RTF Reader version time 2018-09-18 10:52");
        this.maxLineLength = job.getInt("mapred.RTFRecordReader.maxlength", 2147483647);
        this.start = split.getStart();
        if (start != 0) {
            LOG.error("start is not 0 ,return");
            return;
        }
        file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        codecGet(file);
        Path newpath = getPathFromRegex(split.getPath());
        if (isRtFile(newpath)) {
            return;
        }
        FileStatus[] status = fs.globStatus(newpath);
        FSDataInputStream fileIn;
        file = status[0].getPath();//reset the file to prevent the file changes
        loadRTData(trans2Rtf());
        rtDataSize = this.realTimeDataMap.keySet().size();

        fs = FileSystem.get(URI.create(file.toString()), conf);
        fileIn = fs.open(status[0].getPath());
        this.end = start + split.getLength();
        this.end = fs.getLength(status[0].getPath());
        if (this.isCompressedInput()) {
            this.decompressor = CodecPool.getDecompressor(this.codec);
            if (this.codec instanceof SplittableCompressionCodec) {
                SplitCompressionInputStream cIn = ((SplittableCompressionCodec) this.codec)
                        .createInputStream(fileIn, this.decompressor, this.start, this.end,
                                SplittableCompressionCodec.READ_MODE.BYBLOCK);
                this.in = new RTFRecordReader.LineReader(cIn, job);
                this.start = cIn.getAdjustedStart();
                this.end = cIn.getAdjustedEnd();
                this.filePosition = cIn;
            } else {
                this.in = new RTFRecordReader.LineReader(this.codec.createInputStream(fileIn, this
                        .decompressor), job);
                this.filePosition = fileIn;
            }
        } else {
            fileIn.seek(this.start);
            this.in = new RTFRecordReader.LineReader(fileIn, job);
            this.filePosition = fileIn;
        }
        if (start != 0) {// If this is not the first split, we always throw away first record because
            // we always (except the last split) read one extra line in next() method.
            start += in.readLine(new Text(), 0, maxBytesToConsume(start));
        }
        this.pos = start;
    }

    private void loadRTData(String rtfFilename) throws IOException {
        Path path = new Path(rtfFilename);
        FileSystem fs = FileSystem.get(URI.create(rtfFilename), conf);
        FSDataInputStream fsin = fs.open(path);
        BufferedReader br = null;
        String line;
        try {
            br = new BufferedReader(new InputStreamReader(fsin));
            while ((line = br.readLine()) != null) {
                put2realTimeData(this.realTimeDataMap, line.split(Constants.TAB)[0], line);
            }
        } finally {
            br.close();
        }
        LOG.info("RT finish! size: " + this.realTimeDataMap.size());
    }

    private void put2realTimeData(HashMap<String, String> realTimeData, String key, String value) {
        if (!key.equals(Constants.EMPTY_STRING)
                && !value.equals(Constants.EMPTY_STRING)
                && value.split(Constants.TAB).length >= 3) {
            if (realTimeData.keySet().contains(key)) {
                String thisUpdateTS = value.split(Constants.TAB)[1];
                String lastValue = realTimeData.get(key);
                String lastUpdateTS = lastValue.split(Constants.TAB)[1];
                if (1 == Tools.compareTS(thisUpdateTS, lastUpdateTS)) {
                    realTimeData.put(key, value);
                }
            } else {
                realTimeData.put(key, value);
            }
        }
    }

    public void codecGet(Path file) throws ClassNotFoundException {
        this.compressionCodecs = new CompressionCodecFactory(conf);
        this.codec = this.compressionCodecs.getCodec(file);
        Class codecClass = Class.forName(LZO_CODEC_CLASS);
        this.codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
    }

    private static Path getPathFromRegex(Path path) {
        String oldPath = path.toString();
        StringBuffer newpath = new StringBuffer(oldPath.substring(0, oldPath.lastIndexOf("-")))
                .append(Constants.NEW_FILE_TAIL);
        return new Path(newpath.toString());
    }

    private static boolean isRtFile(Path newpath) {
        if (newpath.toString().contains(Constants.RTF_FILE_FLAG)) {
            return true;
        }
        return false;
    }

    private String trans2Rtf() {
        int lastsplit = file.toString().lastIndexOf(Constants.FILE_PATH_SLASH);
        String path = file.toString().substring(0, lastsplit);
        String fileName = file.toString().substring(lastsplit + 1);
        String rtFile = new StringBuffer(path).append(Constants.FILE_PATH_SLASH).append(Constants.RTF_FILE_PREFIX)
                .append(fileName.replace(Constants.HISTORY_DATA_FLAG, Constants.RTF_FILE_FLAG)).toString();
        return rtFile;
    }


    public synchronized boolean hisNext(LongWritable key, Text value) throws IOException {
        cnt++;
        while (this.getFilePosition() < this.end) {
            key.set(this.pos);
            int newSize = this.in.readLine(value, this.maxLineLength, Math.max(this.maxBytesToConsume
                    (this.pos), this.maxLineLength));
            if (value.toString().split(Constants.TAB).length > 1) {
                String primaryKey = value.toString().split(Constants.TAB)[0];
                if (this.realTimeDataMap.containsKey(primaryKey)) {
                    alreadyReadKey.add(primaryKey);
                    String rtValue = this.realTimeDataMap.get(primaryKey);
                    String[] rtValues = rtValue.split(Constants.TAB);
                    String[] values = value.toString().split(Constants.TAB);
                    if (values.length >= 3 && rtValues.length >= 3) {
                        String thisUpdateTS = values[1];
                        String thisOpt = values[2];
                        String rtUpdateTS = rtValues[1];
                        String rtOpt = rtValues[2];
                        if (1 == Tools.compareTS(rtUpdateTS, thisUpdateTS)) {
                            if (rtOpt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
                                value.clear();
                                continue;
                            } else {
                                value.set(rtValue);
                            }
                        } else {
                            if (thisOpt.equals(Constants.OPT_ENUM.DELETE.getValue())) {
                                value.clear();
                            }
                        }
                    }
                }
            } else {
                continue;
            }
            if (newSize == 0) {
                return false;
            }
            String[] values = value.toString().split(Constants.TAB, -1);
            if (values.length >= 3) {
                StringBuffer tmp = new StringBuffer();
                for (int i = 3; i < values.length - 1; i++) {
                    tmp.append(values[i]).append(Constants.TAB);
                }
                tmp.append(values[values.length - 1]);
                value.set(tmp.toString());
            }
            this.pos += (long) newSize;
            if (cnt % 1000000 == 0) {
                LOG.info("***cnt: " + cnt + " ,next()  pos:" + pos + ",value:" + value.toString());
            }

            if (newSize < this.maxLineLength) {
                return true;
            }
            LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - (long) newSize));
        }

        if (this.realTimeDataMap.size() != this.alreadyReadKey.size()) {
            for (String k : this.realTimeDataMap.keySet()) {
                if (!alreadyReadKey.contains(k)) {
                    value.set(realTimeDataMap.get(k));
                    this.alreadyReadKey.add(k);
                    this.pos += (long) value.toString().length();
                    key.set(this.pos);
                    String[] values = value.toString().split(Constants.TAB, -1);
                    if (values.length >= 3) {
                        if (values[2].equals(Constants.OPT_ENUM.DELETE.getValue())) {
                            continue;
                        }
                        StringBuffer tmp = new StringBuffer();
                        for (int i = 3; i < values.length - 1; i++) {
                            tmp.append(values[i]).append(Constants.TAB);
                        }
                        tmp.append(values[values.length - 1]);
                        value.set(tmp.toString());
                    }
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public synchronized boolean next(LongWritable key, Text value) throws IOException {
        if (this.start != 0) {
            return false;
        }
        if (file.toString().contains(Constants.RTF_FILE_FLAG)) {
            return false;
        }
        totalCnt++;
        return hisNext(key, value);
    }

    private boolean isCompressedInput() {
        return this.codec != null;
    }

    private int maxBytesToConsume(long pos) {
        return this.isCompressedInput() ? 2147483647 : (int) Math.min(2147483647L, this.end - pos);
    }

    private long getFilePosition() throws IOException {
        long retVal;
        if (this.isCompressedInput() && null != this.filePosition) {
            retVal = this.filePosition.getPos();
        } else {
            retVal = this.pos;
        }

        return retVal;
    }

    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public float getProgress() throws IOException {
        return this.start == this.end ? 0.0F : Math.min(1.0F, (float) (this.getFilePosition() - this
                .start) / (float) (this.end - this.start));
    }

    @Override
    public synchronized long getPos() throws IOException {
        return this.pos;
    }

    @Override
    public synchronized void close() throws IOException {
        LOG.info("totalCnt: " + totalCnt);
        try {
            if (this.in != null) {
                this.in.close();
            }
        } catch (Exception e) {
            LOG.error("close fail!");
            e.printStackTrace();
        } finally {
            if (this.decompressor != null) {
                CodecPool.returnDecompressor(this.decompressor);
            }
        }
    }

    @Deprecated
    public static class LineReader extends org.apache.hadoop.util.LineReader {
        LineReader(InputStream in) {
            super(in);
        }

        LineReader(InputStream in, int bufferSize) {
            super(in, bufferSize);
        }

        public LineReader(InputStream in, Configuration conf) throws IOException {
            super(in, conf);
        }

    }
}