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

package com.jd.dw.rtf.writer.schedule;

import com.jd.dw.rtf.binlog.model.RtfMessageType;
import com.jd.dw.rtf.writer.Constants;
import com.jd.dw.rtf.writer.Option.RtMapOption;
import com.jd.dw.rtf.writer.tools.LogTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static com.jd.dw.rtf.writer.Constants.*;

public class DirectConsumer {
    private static final Logger logger = LoggerFactory.getLogger(DirectConsumer.class);

    private static String[] fieldArray;
    private static String[] primaryKeyArray;
    static Map<String, String> rtdataMap;


    public String filename;
    public String fields;
    public String primaryKey;
    public String recentlyNDays = "7";
    public RTDataForSchedule rtDataForSchedule;

    public DirectConsumer(String filename, String fields, String primaryKey, RTDataForSchedule rtDataForSchedule) {
        this.filename = filename;
        this.fields = fields;
        this.primaryKey = primaryKey;
        this.rtDataForSchedule = rtDataForSchedule;
    }

    public void preProcess() {
        fieldArray = fields.split(Constants.COMMA, -1);
        primaryKeyArray = primaryKey.split(Constants.COMMA, -1);
    }


    public void process(RtfMessageType record) throws ParseException {

        RtfMessageType data = record;
        long ts = data.getTs();
        String opt = data.getOpt().toString().toLowerCase();
        Map<String, String> rtdataCharMap = new HashMap<String, String>();

        StringBuffer outKey = new StringBuffer();
        StringBuffer value = new StringBuffer();

        if (Constants.UPDATE.equals(opt) || Constants.INSERT.equals(opt)) {
            if (Constants.UPDATE.equals(opt)) {
                opt = OPT_ENUM.UPDATE.getValue();
                rtdataCharMap = data.getSrc();
                for (String k : data.getCur().keySet()) {
                    rtdataCharMap.put(k, data.getCur().get(k));
                }
            } else if (Constants.INSERT.equals(opt)) {
                opt = OPT_ENUM.INSERT.getValue();
                rtdataCharMap = data.getCur();
            }
            rtdataMap = RtMapOption.transform2(rtdataCharMap);

            try {
                for (String index_key : primaryKeyArray) {

                    outKey.append(rtdataMap.get(index_key)).append(Constants.KEY_SPLIT_U0001);
                }

                for (int i = 0; i < fieldArray.length - 1; i++) {
                    if (null == rtdataMap.get(fieldArray[i])) {
                        value.append(NULL).append(Constants.SPLIT_TAB);
                    } else {
                        value.append(RtMapOption.cleanData(rtdataMap.get(fieldArray[i]))).append(Constants.SPLIT_TAB);
                    }
                }

                if (null == rtdataMap.get(fieldArray[fieldArray.length - 1])) {
                    value.append(NULL);
                } else {
                    value.append(RtMapOption.cleanData(rtdataMap.get(fieldArray[fieldArray.length - 1])));
                }
            } catch (Exception e) {
                logger.error("rtdataMap get exception：");
                e.printStackTrace();
            }

            rtDataForSchedule.processRTData(outKey.substring(0, outKey.length() - 1), ts, opt, value
                    .toString(), recentlyNDays);

        } else if (Constants.DELETE.equals(opt)) {
            opt = OPT_ENUM.DELETE.getValue();
            rtdataCharMap = data.getSrc();
            rtdataMap = RtMapOption.transform2(rtdataCharMap);

            for (String index_key : primaryKeyArray) {
                outKey.append(rtdataMap.get(index_key)).append(Constants.KEY_SPLIT_U0001);
            }

            rtDataForSchedule.processRTData(outKey.substring(0, outKey.length() - 1), ts, opt, value
                    .toString(), recentlyNDays);

        }
    }
}