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

package com.jd.dw.rtf.writer.Testing;

import com.jd.dw.rtf.writer.tools.Tools;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author anjinlong
 * @create 2018-04-03 11:41
 * @description description java -cp RTFWriter-JDQ3.0-1.0-SNAPSHOT.jar com.jd.dw.rtf.writer.Testing.TestCompression2 hdfs://ns13/user/dd_edw_rtf/fdm_rtf.db/test_rtf_fdm_pek_orders/99950000-99999999-his-r-01999.rtf
 **/
public class TestCompression2 {


  public static void main(String[] args) throws Exception {
    String filenameWithPath = args[0];
    String recentlyNDays = args[1];

    String cmd = "hadoop jar RTFWriter-JDQ3.0-1.0-SNAPSHOT.jar " + filenameWithPath + " " +
            recentlyNDays;
    long start = System.currentTimeMillis();
    Process p = Runtime.getRuntime().exec(cmd);
    p.waitFor();

    InputStreamReader errInputStreamReader = new InputStreamReader(p.getErrorStream());
    BufferedReader errBufferedReader = new BufferedReader(errInputStreamReader);
    String errStr = null;
    StringBuffer err = new StringBuffer();
    while ((errStr = errBufferedReader.readLine()) != null) {
      if (!errStr.contains("[INFO]")) {
        err.append(errStr);
        err.append("\n");
      }
    }

    try {
//            stdoutBufferedReader.close();
//            stdoutInputStreamReader.close();
      errBufferedReader.close();
      errInputStreamReader.close();
    } catch (Exception e) {
      Tools.printLog("redear 关闭异常");
      e.printStackTrace();
    }

    long finish = System.currentTimeMillis();
    Tools.printLog(new StringBuffer("压完 exit:").append(p.exitValue()).append
            (",用时:").append((finish - start) / 1000).append(",file:")
            .append(filenameWithPath)
            .append(" ; Err:").append(err.toString())
            .toString());//.append(" ; Out:").append(stdout.toString())

    if (p.exitValue() != 0) {
      System.exit(-1);
    }

  }


}