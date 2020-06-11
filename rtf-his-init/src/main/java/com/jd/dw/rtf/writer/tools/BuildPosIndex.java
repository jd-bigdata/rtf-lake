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
import com.jd.dw.rtf.writer.tools.HDFSTools;
import org.apache.directory.api.util.Strings;

import java.io.IOException;
import java.util.Map;

public class BuildPosIndex {
  public static void renameWithLength(String hdfsDir) {
    try {
      Map<String, String> paths = HDFSTools.getFilesUnderFolder(hdfsDir, null);
      paths = HDFSTools.getFilesUnderFolder(hdfsDir, null);
      for (String filepath : paths.keySet()) {
        String fileSize = (String) paths.get(filepath);

        String[] filepaths = filepath.split("/", -1);
        String filename = filepaths[(filepaths.length - 1)];
        if ((!"_SUCCESS".equals(filename)) && (!Strings.isEmpty(filename))) {
          String[] filenames = filename.split("-");
          String startKey = filenames[0];
          String endKey = filenames[1];
          StringBuffer newfilename = new StringBuffer().append(startKey).append(Constants.FILE_NAME_SPLIT).append(endKey).append(Constants.FILE_NAME_SPLIT).append(fileSize);

          String newfilepath = filepath.replace(filename, newfilename);

          String cmd = "hadoop fs -mv " + filepath + " " + newfilepath;

          System.out.println(cmd);
          Runtime.getRuntime().exec(cmd);
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    } catch (IOException e) {
      Map paths;
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    renameWithLength(args[0]);
  }
}