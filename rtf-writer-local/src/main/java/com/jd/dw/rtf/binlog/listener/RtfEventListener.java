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

package com.jd.dw.rtf.binlog.listener;


import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.jd.dw.rtf.binlog.buffer.RealTimeBuffer;
import com.jd.dw.rtf.binlog.model.ColumnInfo;
import com.jd.dw.rtf.binlog.model.RtfMessageType;
import com.jd.dw.rtf.binlog.model.TableInfo;
import com.jd.dw.rtf.binlog.metastore.TableInfoKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * Handle only types of concern
 */
public class RtfEventListener implements BinaryLogClient.EventListener {
    private static final Logger logger = LoggerFactory.getLogger(RtfEventListener.class);

    public RtfEventListener() {
    }

    @Override
    public void onEvent(Event event) {
        if (event == null) {
            logger.error("binlog event is null");
            return;
        }
        EventType eventType = event.getHeader().getEventType();

        if (eventType == EventType.TABLE_MAP) {
            TableMapEventData tableMapEventData = event.getData();
            TableInfoKeeper.saveTableIdMap(tableMapEventData);
            logger.trace("TABLE_MAP_EVENT:tableId:{}", tableMapEventData.getTableId());
        } else if (EventType.isWrite(eventType)) {
            writerEventHandler(event);
        } else if (EventType.isUpdate(eventType)) {
            updateEventHandler(event);
        } else if (EventType.isDelete(eventType)) {
            deleteEventHandler(event);
        }

    }

    /***
     *
     * Event{header=EventHeaderV4{timestamp=1582787722000, eventType=ANONYMOUS_GTID, serverId=1, headerLength=19, dataLength=60, nextPosition=8893, flags=0}, data=null}
     * Event{header=EventHeaderV4{timestamp=1582787722000, eventType=QUERY, serverId=1, headerLength=19, dataLength=56, nextPosition=8968, flags=8}, data=QueryEventData{threadId=42, executionTime=0, errorCode=0, database='test', sql='BEGIN'}}
     * Event{header=EventHeaderV4{timestamp=1582787722000, eventType=TABLE_MAP, serverId=1, headerLength=19, dataLength=38, nextPosition=9025, flags=0}, data=TableMapEventData{tableId=84, database='test', table='test1', columnTypes=3, 15, columnMetadata=0, 300, columnNullability={}, eventMetadata=TableMapEventMetadata{signedness={}, defaultCharset=33, charsetCollations=null, columnCharsets=null, columnNames=null, setStrValues=null, enumStrValues=null, geometryTypes=null, simplePrimaryKeys=null, primaryKeysWithPrefix=null, enumAndSetDefaultCharset=null, enumAndSetColumnCharsets=null}}}
     * Event{header=EventHeaderV4{timestamp=1582787722000, eventType=EXT_WRITE_ROWS, serverId=1, headerLength=19, dataLength=29, nextPosition=9073, flags=0}, data=WriteRowsEventData{tableId=84, includedColumns={0, 1}, rows=[
     *     [19, [B@17d10166]
     * ]}}
     * Write---------------
     * WriteRowsEventData{tableId=84, includedColumns={0, 1}, rows=[
     *     [19, [B@17d10166]
     * ]}
     * Event{header=EventHeaderV4{timestamp=1582787722000, eventType=XID, serverId=1, headerLength=19, dataLength=12, nextPosition=9104, flags=0}, data=XidEventData{xid=252}}
     */
    void writerEventHandler(Event event) {

        WriteRowsEventData writeRowsEventData = event.getData();
        long tableId = writeRowsEventData.getTableId();
        logger.trace("WRITE_ROWS_EVENT:tableId:{}", tableId);

        TableInfo tableInfo = TableInfoKeeper.getTableInfo(tableId);
        String databaseName = tableInfo.getDatabaseName();
        String tableName = tableInfo.getTableName();

        List<Serializable[]> rows = writeRowsEventData.getRows();

        for (Serializable[] row : rows) {
            Map<String, String> afterMap = getMap(row, databaseName, tableName);
            if (afterMap != null && afterMap.size() > 0) {
                RtfMessageType realtimedata = new RtfMessageType(event, databaseName, tableName);
                realtimedata.setOpt("insert");
                realtimedata.setCur(afterMap);
                RealTimeBuffer.queue.addLast(realtimedata);
                logger.info("RealTimeData:{}", realtimedata);
            }
        }
    }

    /***
     * Event{header=EventHeaderV4{timestamp=1582787215000, eventType=ANONYMOUS_GTID, serverId=1, headerLength=19, dataLength=60, nextPosition=8580, flags=0}, data=null}
     * Event{header=EventHeaderV4{timestamp=1582787215000, eventType=QUERY, serverId=1, headerLength=19, dataLength=65, nextPosition=8664, flags=8}, data=QueryEventData{threadId=42, executionTime=0, errorCode=0, database='test', sql='BEGIN'}}
     * Event{header=EventHeaderV4{timestamp=1582787215000, eventType=TABLE_MAP, serverId=1, headerLength=19, dataLength=38, nextPosition=8721, flags=0}, data=TableMapEventData{tableId=84, database='test', table='test1', columnTypes=3, 15, columnMetadata=0, 300, columnNullability={}, eventMetadata=TableMapEventMetadata{signedness={}, defaultCharset=33, charsetCollations=null, columnCharsets=null, columnNames=null, setStrValues=null, enumStrValues=null, geometryTypes=null, simplePrimaryKeys=null, primaryKeysWithPrefix=null, enumAndSetDefaultCharset=null, enumAndSetColumnCharsets=null}}}
     * Event{header=EventHeaderV4{timestamp=1582787215000, eventType=EXT_UPDATE_ROWS, serverId=1, headerLength=19, dataLength=43, nextPosition=8783, flags=0}, data=UpdateRowsEventData{tableId=84, includedColumnsBeforeUpdate={0, 1}, includedColumns={0, 1}, rows=[
     *     {before=[18, [B@17d10166], after=[18, [B@1b9e1916]}
     * ]}}
     * Update--------------
     * UpdateRowsEventData{tableId=84, includedColumnsBeforeUpdate={0, 1}, includedColumns={0, 1}, rows=[
     *     {before=[18, [B@17d10166], after=[18, [B@1b9e1916]}
     * ]}
     * Event{header=EventHeaderV4{timestamp=1582787215000, eventType=XID, serverId=1, headerLength=19, dataLength=12, nextPosition=8814, flags=0}, data=XidEventData{xid=246}}
     *
     */
    void updateEventHandler(Event event) {
        UpdateRowsEventData updateRowsEventData = event.getData();
        long tableId = updateRowsEventData.getTableId();
        logger.trace("UPDATE_ROWS_EVENT:tableId:{}", tableId);

        TableInfo tableInfo = TableInfoKeeper.getTableInfo(tableId);
        String databaseName = tableInfo.getDatabaseName();
        String tableName = tableInfo.getTableName();
        List<Map.Entry<Serializable[], Serializable[]>> rows = updateRowsEventData.getRows();
        for (Map.Entry<Serializable[], Serializable[]> row : rows) {

            Serializable[] beforeRow = row.getKey();
            Serializable[] afterRow = row.getValue();

            Map<String, String> beforeMap = getMap(beforeRow, databaseName, tableName);
            Map<String, String> afterMap = getMap(afterRow, databaseName, tableName);
            if (beforeMap != null && afterMap != null && beforeMap.size() > 0 && afterMap.size() > 0) {
                RtfMessageType realtimedata = new RtfMessageType(event, databaseName, tableName);
                realtimedata.setOpt("update");
                realtimedata.setSrc(beforeMap);
                realtimedata.setCur(afterMap);
                RealTimeBuffer.queue.addLast(realtimedata);
                logger.info("RealTimeData:{}", realtimedata);
            }
        }
    }

    /***
     *
     * Event{header=EventHeaderV4{timestamp=1582787790000, eventType=ANONYMOUS_GTID, serverId=1, headerLength=19, dataLength=60, nextPosition=9183, flags=0}, data=null}
     * Event{header=EventHeaderV4{timestamp=1582787790000, eventType=QUERY, serverId=1, headerLength=19, dataLength=56, nextPosition=9258, flags=8}, data=QueryEventData{threadId=42, executionTime=0, errorCode=0, database='test', sql='BEGIN'}}
     * Event{header=EventHeaderV4{timestamp=1582787790000, eventType=TABLE_MAP, serverId=1, headerLength=19, dataLength=38, nextPosition=9315, flags=0}, data=TableMapEventData{tableId=84, database='test', table='test1', columnTypes=3, 15, columnMetadata=0, 300, columnNullability={}, eventMetadata=TableMapEventMetadata{signedness={}, defaultCharset=33, charsetCollations=null, columnCharsets=null, columnNames=null, setStrValues=null, enumStrValues=null, geometryTypes=null, simplePrimaryKeys=null, primaryKeysWithPrefix=null, enumAndSetDefaultCharset=null, enumAndSetColumnCharsets=null}}}
     * Event{header=EventHeaderV4{timestamp=1582787790000, eventType=EXT_DELETE_ROWS, serverId=1, headerLength=19, dataLength=29, nextPosition=9363, flags=0}, data=DeleteRowsEventData{tableId=84, includedColumns={0, 1}, rows=[
     *     [2, [B@4f8e5cde]
     * ]}}
     * Delete--------------
     * DeleteRowsEventData{tableId=84, includedColumns={0, 1}, rows=[
     *     [2, [B@4f8e5cde]
     * ]}
     * Event{header=EventHeaderV4{timestamp=1582787790000, eventType=XID, serverId=1, headerLength=19, dataLength=12, nextPosition=9394, flags=0}, data=XidEventData{xid=253}}
     */
    void deleteEventHandler(Event event) {
        DeleteRowsEventData deleteRowsEventData = event.getData();
        long tableId = deleteRowsEventData.getTableId();
        logger.trace("DELETE_ROW_EVENT:tableId:{}", tableId);

        TableInfo tableInfo = TableInfoKeeper.getTableInfo(tableId);
        String databaseName = tableInfo.getDatabaseName();
        String tableName = tableInfo.getTableName();
        List<Serializable[]> rows = deleteRowsEventData.getRows();
        for (Serializable[] row : rows) {

            Map<String, String> beforeMap = getMap(row, databaseName, tableName);
            if (beforeMap != null && beforeMap.size() > 0) {
                RtfMessageType realtimedata = new RtfMessageType(event, databaseName, tableName);
                realtimedata.setOpt("delete");
                realtimedata.setCur(beforeMap);
                RealTimeBuffer.queue.addLast(realtimedata);
                logger.info("RealTimeData:{}", realtimedata);
            }
        }
    }


    /**
     * @param
     * @param row
     * @param databaseName
     * @param tableName
     * @return
     */
    private Map<String, String> getMap(Object[] row, String databaseName, String tableName) {
        Map<String, String> map = new HashMap<String, String>();
        if (row == null || row.length == 0) {
            return null;
        }

        String fullName = databaseName + "." + tableName;
        List<ColumnInfo> columnInfoList = TableInfoKeeper.getColumns(fullName);
        if (columnInfoList == null) {
            return null;
        }
        if (columnInfoList.size() != row.length) {
            TableInfoKeeper.refreshColumnsMap();
            if (columnInfoList.size() != row.length) {
                logger.warn("columnInfoList.size is not equal to cols.");
                return null;
            }
        }
        for (int i = 0; i < columnInfoList.size(); i++) {
            map.put(columnInfoList.get(i).getName(),row[i]==null? "" : row[i].toString());
        }
        return map;
    }

}