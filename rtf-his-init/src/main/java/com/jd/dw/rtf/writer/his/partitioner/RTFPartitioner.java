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

package com.jd.dw.rtf.writer.his.partitioner;

import com.jd.dw.rtf.writer.Constants;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author anjinlong
 * @create 2017-07-06 13:58
 * @description description
 **/
public class RTFPartitioner extends Partitioner<Text, Text> {
    public static void main(String[] args) {
        Text key = new Text("1073098481993957376");
        int reduceNum = 592;
        int partition = RTFPartitioner.calPartition(key, reduceNum);
        System.out.println(partition);
        int startKey = (int) (partition * ((double) Constants.PARTITION_FACTOR / reduceNum));
        int endKey = (int) ((partition + 1) * ((double) Constants.PARTITION_FACTOR / reduceNum) - 1);

        StringBuffer fileName = new StringBuffer().append(startKey).append(Constants.FILE_NAME_SPLIT)
                .append(endKey).append(Constants.FILE_NAME_SPLIT).append("his");
        System.out.println(fileName);
    }


    /**
     * Make sure the parkkey is within the scope of the file
     * @param key
     * @param numPartitions
     * @return
     */
    public static int calPartition(Text key, int numPartitions) {
        String k = String.valueOf(key.toString().hashCode()).replace("-", "");
        int partKey;
        if (k.length() <= Constants.PARTITION_KEY_LENGTH) partKey = Integer.parseInt(k);
        else {
            partKey = Integer.parseInt(k.substring(k.length() - Constants.PARTITION_KEY_SUBLENGTH) + k
                    .substring(0, Constants.PARTITION_KEY_SUBLENGTH));
        }

        int partition = 0;
        if (numPartitions == (int) (partKey / (Constants.PARTITION_FACTOR / (double) numPartitions))) {
            partition = numPartitions - 1;
        } else {
            partition = (int) (partKey / (Constants.PARTITION_FACTOR / (double) numPartitions));
        }


        int startKey = (int) (partition * ((double) Constants.PARTITION_FACTOR / numPartitions));
        int endKey = (int) ((partition + 1) * ((double) Constants.PARTITION_FACTOR / numPartitions) - 1);

        if (partKey >= startKey && partKey <= endKey) {
            return partition;
        } else {
            partition = partition - 1;
            startKey = (int) (partition * ((double) Constants.PARTITION_FACTOR / numPartitions));
            endKey = (int) ((partition + 1) * ((double) Constants.PARTITION_FACTOR / numPartitions) - 1);
            if (partKey >= startKey && partKey <= endKey) {
                return partition;
            } else {
                partition = partition + 2;
                startKey = (int) (partition * ((double) Constants.PARTITION_FACTOR / numPartitions));
                endKey = (int) ((partition + 1) * ((double) Constants.PARTITION_FACTOR / numPartitions) - 1);
                if (partKey >= startKey && partKey <= endKey) {
                    return partition;
                }
            }

            return partition;

        }
    }

    public int getPartition(Text key, Text value, int numPartitions) {
        return calPartition(key, numPartitions);
    }
}