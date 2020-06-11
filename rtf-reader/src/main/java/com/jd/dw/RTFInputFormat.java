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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * @author anjinlong
 * @create 2017-06-12 11:20
 * @description description
 **/
public class RTFInputFormat extends FileInputFormat<LongWritable, Text> implements JobConfigurable {
    private static final Log LOG = LogFactory.getLog(RTFInputFormat.class.getName());

    public RTFInputFormat() {
    }

    public void configure(JobConf conf) {
    }

    @Override
    public boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    @Override
    public RecordReader<LongWritable, Text> getRecordReader(
            InputSplit genericSplit, JobConf job, Reporter reporter)
            throws IOException {
        reporter.setStatus(genericSplit.toString());
        try {
            return new RTFRecordReader(job, (FileSplit) genericSplit);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}