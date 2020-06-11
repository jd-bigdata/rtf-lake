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

package com.jd.dw.rtf.writer;


/**
 * @author anjinlong
 * @create 2017-07-06 15:30
 * @description description
 **/
public class Constants {
    public static String ENTER = "\n";
    public static String SPLIT_TAB = "\t";
    public static String STRIKETHROUGH = "-";
    public static String UNDERLINE = "_";
    public static String FILE_PATH_SLASH = "/";
    public static String COMMA = ",";
    public static String DOT = ".";
    public static String RTF_FILE_PREFIX = ".rtf.";
    public static String KEY_SPLIT_U0001 = "\u0001";
    public static String EMPTY_STRING = "";

    public static int PARTITION_KEY_LENGTH = 8;
    public static int PARTITION_KEY_SUBLENGTH = 4;

    public static final String UPDATE = "update";
    public static final String DELETE = "delete";
    public static final String INSERT = "insert";

    public static final String RT_DATA_FLAG = "-rtf-";
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


    public static final String NULL_STRING = "null";

    /**
     * 这些值是可以被启动时修改的
     ***/
    public static long START_COMPACTION_FACTOR = 10;//10M 10485760  52428800
    public static long MUST_COMPACTION_FACTOR = 31457280;
    public static long APPEND_FACTOR = 100000;
    public static int CORE_POOL_SIZE = 10;
    public static int MAX_POOL_SIZE = 20;
    public static int CONSUME_TIME = 5000;
    public static int MAX_COMPACTION_NUM = 10;
    public static int COMPACTION_NUM = 1;

    public static String NULL = "";

    public static void main(String[] args) {
        System.out.println(OPT_ENUM.DELETE.getValue());
    }

}