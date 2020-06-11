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

package com.jd.dw.rtf.writer.his.map;

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


public class HistoryOrcMap extends Mapper<NullWritable, OrcStruct, Text, Text> {
    private static String NULL = "";
    Text idText = new Text();

    public void map(NullWritable key, OrcStruct value, Mapper<NullWritable, OrcStruct, Text, Text>
            .Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String idPosition = conf.get("idPosition");
        int tsPosition = Integer.parseInt(conf.get("tsPosition"));
        int optPosition = Integer.parseInt(conf.get("optPosition"));
        int fieldStartPosition = Integer.parseInt(conf.get("fieldStartPosition"));
        String fieldDropIndexs = conf.get("fieldDropIndexs");
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
        outValue.append(outKey).append(Constants.SPLIT_TAB);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        if (tsPosition != -1) {
            String Ts_pos_value = String.valueOf(value.getFieldValue(tsPosition));
            if ((Ts_pos_value == null) || (Ts_pos_value == "null")) {
                try {
                    date = simpleDateFormat.parse(Constants.INIT_TIME);
                    outValue.append(String.valueOf(date.getTime())).append(Constants.SPLIT_TAB);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            } else if ((Ts_pos_value != "") && (!Tools.isNumeric(Ts_pos_value))) {
                try {
                    date = simpleDateFormat.parse(Ts_pos_value);
                    outValue.append(String.valueOf(date.getTime())).append(Constants.SPLIT_TAB);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            } else {
                outValue.append(Ts_pos_value).append(Constants.SPLIT_TAB);
            }
        } else {
            try {
                date = simpleDateFormat.parse(Constants.INIT_TIME);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            outValue.append(String.valueOf(date.getTime())).append(Constants.SPLIT_TAB);
        }
        if (optPosition != -1) {
            String opt_pos_value = String.valueOf(value.getFieldValue(optPosition));

            if (opt_hashmap.containsKey(opt_pos_value)) {
                outValue.append((String) opt_hashmap.get(opt_pos_value));
            } else {
                outValue.append(value.getFieldValue(optPosition)).append(Constants.SPLIT_TAB);
            }
        } else {
            outValue.append("").append(Constants.SPLIT_TAB);
        }

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

        context.write(new Text(outKey.toString()), new Text(outValue.toString()));//bug fix key
    }
}