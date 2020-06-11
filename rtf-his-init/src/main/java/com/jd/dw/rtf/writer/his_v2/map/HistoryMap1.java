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

package com.jd.dw.rtf.writer.his_v2.map;

import com.jd.dw.rtf.writer.Constants;
import com.jd.dw.rtf.writer.tools.Tools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.orc.mapred.OrcStruct;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;


public class HistoryMap1 extends Mapper<NullWritable, OrcStruct, Text, Text> {
    private static String NULL = "";
    Text idText = new Text();
    private static String yesterday;
    String idPosition;
    int tsPosition;
    int recentlyNDays;
    int fieldStartPosition;
    String fieldDropIndexs;
    String lastNDate;


    @Override
    protected void setup(Mapper<NullWritable, OrcStruct, Text, Text>.Context context)
            throws IOException, InterruptedException {

        yesterday = Tools.getDateBefore(1);
        Configuration conf = context.getConfiguration();
        idPosition = conf.get("idPosition");
        tsPosition = Integer.parseInt(conf.get("tsPosition"));
        recentlyNDays = Integer.parseInt(conf.get("recentlyNDays"));
        fieldStartPosition = Integer.parseInt(conf.get("fieldStartPosition"));
        fieldDropIndexs = conf.get("fieldDropIndexs");
        lastNDate = Tools.getDateBefore(recentlyNDays);

        System.out.println("参数： idPosition:" + idPosition + " | tsPosition: " + tsPosition
                + "  |  recentlyNDays: " + recentlyNDays + " | fieldStartPosition:" + fieldStartPosition
                + " | fieldDropIndexs:" + fieldDropIndexs);
        System.out.println(" *** lastNDate: " + lastNDate);

    }

    public void map(NullWritable key, OrcStruct value, Mapper<NullWritable, OrcStruct, Text, Text>
            .Context context) throws IOException, InterruptedException {

        HashSet<Integer> fieldDropSet = new HashSet<Integer>();
        for (String index : fieldDropIndexs.split(",", -1)) {
            fieldDropSet.add(Integer.parseInt(index));
        }
        HashMap opt_hashmap = Constants.opt_hashmap();
        StringBuffer outKey = new StringBuffer();
        StringBuffer outValue = new StringBuffer();
        if (idPosition != "-1") {
            String[] idPoss = idPosition.split(",", -1);
            if (idPoss.length == 1) {
                outKey.append(value.getFieldValue(Integer.parseInt(idPosition)));

            } else {
                for (int i = 0; i < idPoss.length - 1; i++) {
                    outKey.append(value.getFieldValue(Integer.parseInt(idPoss[i]))).append(Constants
                            .KEY_SPLIT_U0001);
                }
                outKey.append(value.getFieldValue(Integer.parseInt(idPoss[idPoss.length - 1])));

            }
        } else {
            outKey.append("");
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date updateDate = null;
        String Ts_pos_value = "";
        if (tsPosition != -1) {
            Ts_pos_value = String.valueOf(value.getFieldValue(tsPosition));
            if ((Ts_pos_value == null) || (Ts_pos_value == "null"
                    || (Ts_pos_value == "") || Ts_pos_value.length() == 0)) {
                try {
                    updateDate = simpleDateFormat.parse(Constants.INIT_TIME);
                    outValue.append(String.valueOf(updateDate.getTime())).append(Constants.SPLIT_TAB);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            } else if ((Ts_pos_value != "") && (!Tools.isNumeric(Ts_pos_value))) {
                try {
                    updateDate = simpleDateFormat.parse(Ts_pos_value);
                    outValue.append(String.valueOf(updateDate.getTime())).append(Constants.SPLIT_TAB);
                } catch (ParseException e) {
                    e.printStackTrace();
                    try {
                        updateDate = simpleDateFormat.parse(Constants.INIT_TIME);
                        outValue.append(String.valueOf(updateDate.getTime())).append(Constants.SPLIT_TAB);
                    } catch (ParseException e1) {
                        e1.printStackTrace();
                    }
                }
            } else {
                if (Ts_pos_value.length() == 10) {
                    Ts_pos_value = Ts_pos_value + "000";
                }
                try {
                    updateDate = simpleDateFormat.parse(Tools.timestamp2String(Long.parseLong(Ts_pos_value)));
                } catch (Exception e) {
                    System.out.println(" error Ts_pos_value :" + Ts_pos_value);
                    e.printStackTrace();
                }
                outValue.append(Ts_pos_value).append(Constants.SPLIT_TAB);
            }
        } else {
            try {
                updateDate = simpleDateFormat.parse(yesterday);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            outValue.append(String.valueOf(updateDate.getTime())).append(Constants.SPLIT_TAB);
        }

        outValue.append(Constants.SPLIT_TAB);

        for (int i = fieldStartPosition; i < value.getNumFields() - 1; i++) {

            if (!fieldDropSet.contains(i)) {
                if (null == value.getFieldValue(i)) {
                    outValue.append(NULL).append(Constants.SPLIT_TAB);
                } else {
                    outValue.append(value.getFieldValue(i)).append(Constants.SPLIT_TAB);
                }
            }
        }

        if (null == value.getFieldValue(value.getNumFields() - 1)) {
            outValue.append(NULL);
        } else {
            outValue.append(value.getFieldValue(value.getNumFields() - 1));
        }
        if (1 == Tools.compareTS(String.valueOf(updateDate.getTime()), lastNDate)) {
            context.write(new Text(outKey.toString()), new Text(outValue.toString()));
        }
    }
}