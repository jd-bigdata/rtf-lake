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

package com.jd.dw.rtf.binlog.metastore;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.jd.dw.rtf.binlog.model.ColumnInfo;
import com.jd.dw.rtf.binlog.model.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableInfoKeeper {

    private static final Logger logger = LoggerFactory.getLogger(TableInfoKeeper.class);

    private static Map<Long, TableInfo> tabledIdMap = new ConcurrentHashMap<Long, TableInfo>();
    private static Map<String, List<ColumnInfo>> columnsMap = new ConcurrentHashMap<String, List<ColumnInfo>>();

    static {
        columnsMap = MysqlConnection.getColumns();
    }

    public static void saveTableIdMap(TableMapEventData tableMapEventData) {
        long tableId = tableMapEventData.getTableId();
        tabledIdMap.remove(tableId);

        TableInfo table = new TableInfo();
        table.setDatabaseName(tableMapEventData.getDatabase());
        table.setTableName(tableMapEventData.getTable());
        table.setFullName(tableMapEventData.getDatabase() + "." + tableMapEventData.getTable());

        tabledIdMap.put(tableId, table);
    }

    public static synchronized void refreshColumnsMap() {
        Map<String, List<ColumnInfo>> map = MysqlConnection.getColumns();
        if (map.size() > 0) {
            columnsMap = map;
        } else {
            logger.error("refresh columnsMap error.");
        }
    }

    public static TableInfo getTableInfo(long tableId) {
        return tabledIdMap.get(tableId);
    }

    public static List<ColumnInfo> getColumns(String fullName) {
        return columnsMap.get(fullName);
    }
}