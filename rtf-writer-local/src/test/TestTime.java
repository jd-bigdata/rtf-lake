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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author anjinlong
 * @create 2018-06-14 17:28
 * @description description
 **/
public class TestTime {

  public static void main(String[] args) throws Exception{
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    Date dt1 = df.parse("2018-08-27 01:30:00");
    long l1 = dt1.getTime();
    System.out.println(l1);
    System.out.println((l1-System.currentTimeMillis())/1000);

    System.out.println("long : "+Long.MAX_VALUE);

  }

}