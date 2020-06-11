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

package com.jd.dw.rtf.writer.ConsumeSaved;

import com.jd.dw.rtf.writer.schedule.DirectConsumer;
import com.jd.dw.rtf.writer.schedule.RTDataForSchedule;
import com.jd.dw.rtf.writer.tools.LogTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;

public class ConsumerSave {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerSave.class);
    public ArrayList<String> primaryKeyList;
    public ArrayList<String> fieldsList;
    public ArrayList<String> fileNameList;

    public ConsumerSave(ArrayList<String> dbTableList, ArrayList<String> primaryKeyList, ArrayList<String> fieldsList) {
        this.fileNameList = dbTableList;
        this.primaryKeyList = primaryKeyList;
        this.fieldsList = fieldsList;
    }

    public ArrayList<DirectConsumer> createConsumer() throws IOException {
        final ArrayList<DirectConsumer> directConsumers = new ArrayList<>();
        if (fileNameList.size() != primaryKeyList.size() || primaryKeyList.size() != fieldsList.size()) {
            logger.error("Please check whether the input table name, field name and primary key name are missing");
            return null;
        } else {
            for (int i = 0; i < fileNameList.size(); i++) {
                directConsumers.add(new DirectConsumer(fileNameList.get(i), fieldsList.get(i), primaryKeyList.get(i)
                        , new RTDataForSchedule(fileNameList.get(i))));
                directConsumers.get(i).preProcess();
            }
        }
        return directConsumers;
    }
}