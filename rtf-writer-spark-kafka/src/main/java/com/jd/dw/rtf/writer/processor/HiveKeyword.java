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

package com.jd.dw.rtf.writer.processor;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;

/**
 * @author anjinlong
 * @create 2018-04-25 15:42
 * @description description
 **/
public class HiveKeyword {
  public static final String PREFIX = "v_";

  //_id_
  public static String[] hiveKeyword = {"join", "select", "if", "else", "function", "map",
          "column", "where", "dt", "jrdw_timestamp", "from", "end", "exchange", "rank",
          "key",
          "group", "div", "start_date", "end_date", "change_code", "deleted", "bigint", "double",
          "string", "date", "comment", "percent", "order", "sort"};

  public static HashMap<String, String> hiveKeywordMap;

  static {
    hiveKeywordMap = new HashMap<String, String>();
    for (String s : hiveKeyword) {
      hiveKeywordMap.put(new StringBuffer(PREFIX).append(s).toString(), s);
    }
  }

  public static String replace(String key) {
    if (hiveKeywordMap.keySet().contains(key)) {
      return hiveKeywordMap.get(key);
    } else {
      return key;
    }
  }

  public static String[] replaceArray(String[] fields) {
    for (int i = 0; i < fields.length; i++) {
      fields[i] = replace(fields[i]);
    }
    return fields;
  }


  public static void main(String[] args) {
    String[] a = {"a", "v_deleted", "c"};

    a = replaceArray(a);
    for (String tmp : a) {
      System.out.println(tmp);
    }

    System.out.println(StringUtils.join(a,","));

  }


}