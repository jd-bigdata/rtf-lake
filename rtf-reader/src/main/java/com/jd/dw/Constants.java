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

/**
 * @author anjinlong
 * @create 2017-06-12 16:48
 * @description description
 **/
public class Constants {

    public static final String NULL_STRING = "null";
    public static final String TAB = "\t";
    public static final String EMPTY_STRING = "";
    public static String STRIKETHROUGH = "-";
    public static String UNDERLINE = "_";
    public static String FILE_PATH_SLASH = "/";


    public static final String UPDATE = "update";
    public static final String DELETE = "delete";
    public static final String INSERT = "insert";

    public static String RTF_FILE_PREFIX = ".rtf.";
    public static final String RTF_FILE_FLAG = "-rtf-";
    public static final String NEW_FILE_TAIL = "-*";
    public static final String HISTORY_DATA_FLAG = "-his-";
    public static final String TEXT_FILE_SUBFIX = ".txt";
    public static final String LZO_FILE_SUBFIX = ".lzo";

    public static enum OPT_ENUM {
        INSERT("0"), DELETE("1"), UPDATE("2");

        private final String value;

        OPT_ENUM(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public static final String RTF_END = "_rtf";


    public static void main(String[] args) {
        System.out.println(OPT_ENUM.DELETE.getValue());
    }


}