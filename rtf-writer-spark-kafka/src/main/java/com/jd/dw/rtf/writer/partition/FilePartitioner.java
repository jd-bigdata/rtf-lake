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

package com.jd.dw.rtf.writer.partition;

import org.apache.spark.Partitioner;

public class FilePartitioner extends Partitioner {

  public FilePartitioner(int partitionNum){
    this.partitionNum = partitionNum;
  }

  private int partitionNum = 500;

  private int partitions;

  private final static String FILENAME_SEP="-";

  private final static String FILENAME_END=".rtf";

  private final static String BLANK_STRING="";

  @Override
  public int numPartitions() {
    return partitionNum;
  }
  @Override
  public int getPartition(Object key) {
    // 99000000-99199999-his-r-00495.rtf
    if (key == null ) {
      return 0;
    }
    try {
      String[] arr = key.toString().split(FILENAME_SEP);
      String tmpStrPartition = arr[arr.length - 1].replace(FILENAME_END, BLANK_STRING);
      partitions = Integer.parseInt(tmpStrPartition);
      partitions = partitions % partitionNum;
      return partitions;
    }catch (Exception e){
      return 0;
    }
  }

  public boolean equals(Object obj) {
    if (obj instanceof FilePartitioner) {
      return ((FilePartitioner) obj).partitions == partitions;
    }
    return false;
  }

}