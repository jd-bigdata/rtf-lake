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

package com.jd.dw.rtf.writer.his_v2;

import com.hadoop.mapreduce.LzoTextInputFormat;
import com.jd.dw.rtf.writer.Constants;
import com.jd.dw.rtf.writer.his.partitioner.RTFPartitioner;
import com.jd.dw.rtf.writer.his_v2.map.HistoryMap1;
import com.jd.dw.rtf.writer.his_v2.map.HistoryMap2;
import com.jd.dw.rtf.writer.his_v2.reduce.HistoryReduce2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.orc.mapreduce.OrcInputFormat;

/**
 * @author anjinlong
 * @create 2018-11-23 16:12
 * @description description
 **/
public class HistoryInitV2 {

    public static void main(String[] args) throws Exception {
        System.out.println("please input: " +
                "input 、\n" +
                "output、\n" +
                "idPosition、\n" +
                "tsPosition、\n" +
                "recentlyNDays、\n" +
                "fieldStartPosition、\n" +
                "fieldDropIndexs"
        );
        if (args.length < 4) {
            System.out.println("please check your args!!");
            System.exit(255);
        }
        String inputPath = args[0];
        String outputPath = args[1];
        String idPosition = args[2];
        String tsPosition = args[3];
        String recentlyNDays = "7";
        String fieldStartPosition = "0";
        String fieldDropIndexs = "-1";

        if (args.length > 4) {
            recentlyNDays = args[4];
        }
        if (args.length > 5) {
            fieldStartPosition = args[5];
        }
        if (args.length > 6) {
            fieldDropIndexs = args[6];
        }
        System.out.println("inputPath: " + inputPath);
        System.out.println("outputPath: " + outputPath);
        System.out.println("idPosition: " + idPosition);
        System.out.println("tsPosition: " + tsPosition);
        System.out.println("recentlyNDays: " + recentlyNDays);
        System.out.println("fieldStartPosition: " + fieldStartPosition);
        System.out.println("fieldDropIndexs:" + fieldDropIndexs);

        Configuration conf = new Configuration();
        if (idPosition != "-1") {
            conf.set("idPosition", idPosition);
        } else {
            conf.set("idPosition", "-1");
        }
        if (tsPosition != "-1") {
            conf.set("tsPosition", tsPosition);
        } else {
            conf.set("tsPosition", "-1");
        }
        conf.set("recentlyNDays", recentlyNDays);
        conf.set("fieldStartPosition", fieldStartPosition);
        conf.set("fieldDropIndexs", fieldDropIndexs);
        conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
        conf.set("mapred.output.compress", "true");


        String OutputPathJob1;
        if (outputPath.lastIndexOf("/") == outputPath.length() - 1) {
            OutputPathJob1 = outputPath.substring(0, outputPath.length() - 1) + "_tmp1";
        } else {
            OutputPathJob1 = outputPath + "_tmp1";
        }

        /** job1 **/
        Job job1 = new Job(conf, "HistoryInitV2-job1");
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setMapperClass(HistoryMap1.class);
        job1.setInputFormatClass(OrcInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(OutputPathJob1));

        LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
        job1.setNumReduceTasks(200);
        job1.setJarByClass(HistoryInitV2.class);
        job1.waitForCompletion(true);
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(OutputPathJob1);
        System.out.println("Length:" + fs.getContentSummary(path).getLength());
        int reduceNum = (int) (fs.getContentSummary(path).getLength() / (1024 * 1024 * 128));
        if (reduceNum == 0) {
            reduceNum = 1;
        }
        System.out.println("reduceNum:" + reduceNum);


        /** job2 **/

        conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
        conf.set("mapred.output.compress", "true");
        conf.set("reduceNum", String.valueOf(reduceNum));

        Job job2 = new Job(conf, "HistoryInitV2-job2");
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapperClass(HistoryMap2.class);
        job2.setReducerClass(HistoryReduce2.class);
        job2.setInputFormatClass(LzoTextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setPartitionerClass(RTFPartitioner.class);
        FileInputFormat.addInputPath(job2, new Path(OutputPathJob1));
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));
        LazyOutputFormat.setOutputFormatClass(job2, TextOutputFormat.class);
        job2.setNumReduceTasks(reduceNum);
        job2.setJarByClass(HistoryInitV2.class);
        job2.waitForCompletion(true);
        fs.delete(new Path(OutputPathJob1));
        try {
            path = new Path(outputPath);
            if (fs.exists(path)) {
                FileStatus[] fileStatus = fs.listStatus(path);
                for (int i = 0; i < fileStatus.length; i++) {
                    FileStatus fileStatu = fileStatus[i];
                    if (!fileStatu.isDir()) {
                        if (fileStatu.getPath().getName().contains(Constants.HIS_FLAG_STR)) {
                            String oldFilename = fileStatu.getPath().getName();
                            String newFilename = oldFilename.split("\\.", -1)[0] + ".rtf";
                            fs.rename(new Path(outputPath + "/" + oldFilename)
                                    , new Path(outputPath + "/" +
                                    newFilename));
                        }
                    }
                }
            }
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}