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

package com.jd.dw.rtf.writer.his_v2.reduce;

import com.jd.dw.rtf.writer.Constants;
import com.jd.dw.rtf.writer.his.partitioner.RTFPartitioner;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * @author anjinlong
 * @create 2017-07-06 10:42
 * @description description
 **/
public class HistoryReduce2 extends Reducer<Text, Text, NullWritable, Text> {
    private static String REDUCE_NUM_STR = "reduceNum";
    private static String HIS_STR = "his";

    private static String NOT_MATCH_FILE = "not-match.rtf";

    private MultipleOutputs<NullWritable, Text> multipleOutputs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        int reduceNum = Integer.parseInt(context.getConfiguration().get(REDUCE_NUM_STR));
        int partition = RTFPartitioner.calPartition(key, reduceNum);
        int startKey = (int) (partition * ((double) Constants.PARTITION_FACTOR / reduceNum));
        int endKey = (int) ((partition + 1) * ((double) Constants.PARTITION_FACTOR / reduceNum) - 1);
        StringBuffer fileName;
        String k = String.valueOf(key.toString().hashCode()).replace("-", "");
        int partKey;
        if (k.length() <= Constants.PARTITION_KEY_LENGTH) partKey = Integer.parseInt(k);
        else {
            partKey = Integer.parseInt(k.substring(k.length() - Constants.PARTITION_KEY_SUBLENGTH) + k
                    .substring(0, Constants.PARTITION_KEY_SUBLENGTH));
        }

        if (partKey >= startKey && partKey <= endKey) {
            fileName = new StringBuffer().append(startKey).append(Constants.FILE_NAME_SPLIT)
                    .append(endKey).append(Constants.FILE_NAME_SPLIT).append(HIS_STR);
        } else {
            fileName = new StringBuffer(NOT_MATCH_FILE);
        }
        for (Text val : values) {
            multipleOutputs.write(NullWritable.get(), val, fileName.toString());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

}