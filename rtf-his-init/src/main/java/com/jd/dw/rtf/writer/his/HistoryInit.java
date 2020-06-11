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

package com.jd.dw.rtf.writer.his;

import com.jd.dw.rtf.writer.Constants;
import com.jd.dw.rtf.writer.his.map.HistoryOrcMap;
import com.jd.dw.rtf.writer.his.partitioner.RTFPartitioner;
import com.jd.dw.rtf.writer.his.reduce.HistoryReduce;
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

import java.net.URI;

/**
 * @author anjinlong
 * @create 2017-07-05 16:12
 * @description description
 **/
public class HistoryInit {

    public static void main(String[] args) throws Exception {
        System.out.println("please input: reduceNum、inputPath、outputPath、idPosition、tsPosition、optPosition、fieldStartPosition" +
                "、fieldDropIndexs。");
        System.out.println("/** idPosition: field order of primary key in the table (starting from 0)\n" +
                "         * tsPosition: field order of update time in the table\n" +
                "         * optPosition: operation type (not - 1)\n" +
                "         * fieldStartPosition: the column at the beginning of the data, that is, the first few fields do not want to\n" +
                "         * */");
        if (args.length < 6) {
            System.out.println("please check your args!!");
            System.exit(255);
        }

        int reduceNum = Integer.parseInt(args[0]);
        String inputPath = args[1];
        String outputPath = args[2];

        //The subscript value of the primary key field, starting from 0. The primary keys of multiple fields are
        // separated by commas, for example, 2,3,5
        String idPosition = args[3];

        //Update the field subscript value (Updatetime, modify). Use - 1 if not found
        String tsPosition = args[4];

        //Operation type defaults to - 1
        String optPosition = args[5];

        //Start field subscript, if there is start_ date,change_ Code and other fields need to be removed
        String fieldStartPosition = "0";

        //Fields that do not end, multiple fields separated by commas
        String fieldDropIndexs = "-1";
        if (args.length > 6) {
            fieldStartPosition = args[6];

        }
        if (args.length > 7) {
            fieldDropIndexs = args[7];
        }

        System.out.println("reduceNum: " + reduceNum);
        System.out.println("inputPath: " + inputPath);
        System.out.println("outputPath: " + outputPath);
        System.out.println("idPosition: " + idPosition);
        System.out.println("tsPosition: " + tsPosition);
        System.out.println("optPosition: " + optPosition);
        System.out.println("fieldStartPosition: " + fieldStartPosition);
        System.out.println("fieldDropIndexs:" + fieldDropIndexs);


        Configuration conf = new Configuration();
        conf.set("reduceNum", String.valueOf(reduceNum));
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

        if (optPosition != "-1") {
            conf.set("optPosition", optPosition);
        } else {
            conf.set("optPosition", "-1");
        }

        conf.set("fieldStartPosition", fieldStartPosition);
        conf.set("fieldDropIndexs", fieldDropIndexs);

        conf.set("mapred.output.compression.codec", "com.hadoop.compression.lzo.LzopCodec");
        conf.set("mapred.output.compress", "true");

        Job job = new Job(conf, "HistoryInit");
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(HistoryOrcMap.class);
        job.setReducerClass(HistoryReduce.class);
        job.setInputFormatClass(OrcInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(RTFPartitioner.class);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        job.setNumReduceTasks(reduceNum);
        job.setJarByClass(HistoryInit.class);
        job.waitForCompletion(true);

        try {
            FileSystem fs = FileSystem.get(URI.create(outputPath), conf);

            Path path = new Path(outputPath);
            if (fs.exists(path)) {
                FileStatus[] fileStatus = fs.listStatus(path);
                for (int i = 0; i < fileStatus.length; i++) {
                    FileStatus fileStatu = fileStatus[i];
                    if (!fileStatu.isDir()) {
                        if (fileStatu.getPath().getName().contains(Constants.HIS_FLAG_STR)) {
                            String oldFilename = fileStatu.getPath().getName();
                            String newFilename = oldFilename.split("\\.", -1)[0] + ".rtf";
                            fs.rename(new Path(outputPath + "/" + oldFilename), new Path(outputPath + "/" +
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