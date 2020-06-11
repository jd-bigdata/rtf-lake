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

package com.jd.dw.rtf.writer.Option;

import com.jd.dw.rtf.writer.Constants;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author anjinlong
 * @Date 2020/2/24 2:57
 * @Version 1.0
 */
public class RtMapOption {
    private final static String ENTER_R = "\n";

    public static Map<String, String> transform2(Map<String, String> srcMap) {
        if (srcMap == null) {
            return null;
        } else {
            Map<String, String> destMap = new HashMap();
            Iterator it = srcMap.entrySet().iterator();

            while (it.hasNext()) {
                Map.Entry<CharSequence, CharSequence> e = (Map.Entry) it.next();
                String key = e.getKey() == null ? null : ((CharSequence) e.getKey()).toString();
                key = key.toLowerCase();
                String value = e.getValue() == null ? null : ((CharSequence) e.getValue()).toString();
                destMap.put(key, value);
            }
            return destMap;
        }
    }

    /**
     * @param data
     * @return
     */
    public static String cleanData(String data) {
        if (null == data || StringUtils.isEmpty(data)) {
            return data;
        } else {
            return data.replace(Constants.SPLIT_TAB, Constants.EMPTY_STRING)
                    .replace(ENTER_R, Constants.EMPTY_STRING)
                    .replace(Constants.ENTER, Constants.EMPTY_STRING);
        }
    }
}