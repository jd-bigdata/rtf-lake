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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author anjinlong
 * @create 2017-07-05 16:12
 * @description description
 **/
public class HistoryRename {

  public static void main(String[] args) throws Exception {
    String outputPath = args[0];

    System.out.println("outputPath: " + outputPath);

    Configuration conf = new Configuration();

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
            /**
             else{
             String oldFilename = fileStatu.getPath().getName();
             String newFilename = oldFilename.replace("txt","rtf");

             System.out.println("老文件：" + outputPath + "/" + oldFilename);
             System.out.println("新文件：" + outputPath + "/" + newFilename);

             fs.rename(new Path(outputPath + "/" + oldFilename), new Path(outputPath + "/" +
             newFilename));
             }
             **/
          }
        }
      }
      fs.close();
    } catch (Exception e) {
      e.printStackTrace();
    }


  }
}