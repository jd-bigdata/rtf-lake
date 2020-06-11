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
import org.apache.commons.lang.StringUtils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * @author anjinlong
 * @create 2017-07-11 11:39
 * @description
 **/
public class DateTools {

    final static String DATE_FORMATE = "yyyy-MM-dd hh:mm:ss";

    public static int compareTS(String ts1, String ts2) throws ParseException {
        if (StringUtils.isEmpty(ts1) || Constants.NULL_STRING.equals(ts1)) {
            return 0;
        }
        if (StringUtils.isEmpty(ts2) || Constants.NULL_STRING.equals(ts2)) {
            return 1;
        }

        return parseTime(ts1) >= parseTime(ts2) ? 1 : 0;
    }

    public static String timestamp2String(long ts) {
        String tsStr = "";
        DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        try {
            tsStr = sdf.format(ts);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tsStr;
    }

    public static String getCurrentTime() {
        return timestamp2String(System.currentTimeMillis());
    }

    /**
     * @param lastNDays
     * @return
     */
    public static String getDateBefore(int lastNDays) {
        Calendar now = Calendar.getInstance();
        now.set(Calendar.DATE, now.get(Calendar.DATE) - lastNDays);
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMATE);
        return sdf.format(now.getTime());
    }

    public static Long parseTime(String datetime) throws ParseException {
        if (datetime.contains(":")) {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date parse = df.parse(datetime);
            return parse.getTime();
        } else {
            return Long.parseLong(datetime);
        }
    }

    public static void main(String[] args) {
        System.out.println(getDateBefore(1));

        try {
            Date d = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String dateNowStr = sdf.format(d);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}