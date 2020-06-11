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



import org.apache.commons.lang.StringUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import static com.jd.dw.Constants.RTF_END;

/**
 * @author anjinlong
 * @create 2017-06-12 16:41
 * @description
 **/
public class Tools {

  public static void main(String[] args) {
    String str = "{00000000500000080000000002001000030100100000000000100000000000000000000000000000000000000000000000000" +
            "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000, 10001080912," +
            " wdNXBGnHpfSUOYd,  ,  , 12, 984, 40035, 0,                     , ,  ,  ,  , 1, 70, , 2015-09-01 18:46:00.0, " +
            "9, 0, 2015-09-01 18:46:00.0, , , 1, 39.6,  , 10.0, 0.0, vb06767482536, 京东快递, 2015-09-02 13:21:07.0,  , 0," +
            " , 2015-09-02 13:21:07.0, , , 0.0, 0.0, 0.0, 0.0, 543, 0, 22, 0, , 2015-09-02 13:21:00.0, 1, , , " +
            ", 2015-09-01 18:46:26.293, , , , , 0, , 0, , , , , , , 0, 0, 3, , , 0, , 10001080912, 0, 0, 3, 2015-09-02" +
            ", 2, , 0, , 10001080912, -28800000,  }";
    String[] strs = str.split(",", -1);
    System.out.println(strs[strs.length - 3]);
    System.out.println(strs[strs.length - 2]);
    System.out.println(strs[strs.length - 1].replace("}", ""));


  }

  public static String getRtFilename(String hisName) {
    return hisName + RTF_END;
  }

  /**
   * @param ts1
   * @param ts2
   * @return ts1>=ts2 return 1，else 0
   */
  public static int compareTS(String ts1, String ts2) {
    if (StringUtils.isEmpty(ts1) || Constants.NULL_STRING.equals(ts1)) {
      return 0;
    }
    if (StringUtils.isEmpty(ts2) || Constants.NULL_STRING.equals(ts2)) {
      return 1;
    }
    long l1 = 0;
    long l2 = 0;
    try {
      if (ts1.contains(":")) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date dt1 = df.parse(ts1);
        l1 = dt1.getTime();
      } else {
        l1 = Long.parseLong(ts1);
      }

      if (ts2.contains(":")) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Date dt2 = df.parse(ts2);
        l2 = dt2.getTime();
      } else {
        l2 = Long.parseLong(ts2);
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    if (l1 >= l2) {
      return 1;
    } else {
      return 0;
    }
  }

  public static int compare_date(String DATE1, String DATE2) {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    try {
      Date dt1 = df.parse(DATE1);
      Date dt2 = df.parse(DATE2);
      if (dt1.getTime() >= dt2.getTime()) {
        return 1;
      } else if (dt1.getTime() < dt2.getTime()) {
        return 0;
      }
    } catch (Exception exception) {
      exception.printStackTrace();
    }
    return 1;
  }
}