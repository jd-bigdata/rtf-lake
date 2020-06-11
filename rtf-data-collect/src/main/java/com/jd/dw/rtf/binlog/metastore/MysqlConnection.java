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


import com.jd.dw.rtf.binlog.model.ColumnInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlConnection {
    private static final Logger logger = LoggerFactory.getLogger(MysqlConnection.class);

    private static Connection conn;

    private static String host;
    private static int port;
    private static String user;
    private static String password;

    public static void setConnection(String hostArg, int portArg, String userArg, String passwordArg){
        try {
            if(conn == null || conn.isClosed()){
                Class.forName("com.mysql.cj.jdbc.Driver");//com.mysql.jdbc.Driver

                host = hostArg;
                port = portArg;
                user = userArg;
                password = passwordArg;

                conn = DriverManager.getConnection("jdbc:mysql://"+host+":"+port+"/",user,password);
                logger.debug("connected to mysql:{} : {}",user,password);
            }
        } catch (ClassNotFoundException e) {
            logger.debug(e.getMessage(),e);
        } catch (SQLException e) {
            logger.debug(e.getMessage(),e);
        }
    }

    public static Connection getConnection(){
        try {
            if(conn == null || conn.isClosed()){
                setConnection(host,port,user,password);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(),e);
        }
        return conn;
    }

    /**
     * 获取Column信息
     * 拿到所有的database，table，和column名
     *
     * @return
     */
    public static Map<String, List<ColumnInfo>> getColumns(){
        Map<String,List<ColumnInfo>> cols = new HashMap<String,List<ColumnInfo>>();
        Connection conn = getConnection();

        try {
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet r = metaData.getCatalogs();
            String tableType[] = {"TABLE"};
            while(r.next()){
                String databaseName = r.getString("TABLE_CAT");
                ResultSet result = metaData.getTables(databaseName, null, null, tableType);
                while(result.next()){
                    String tableName = result.getString("TABLE_NAME");
                    String key = databaseName +"."+tableName;
                    ResultSet colSet = metaData.getColumns(databaseName, null, tableName, null);
                    cols.put(key, new ArrayList<ColumnInfo>());
                    while(colSet.next()){
                        ColumnInfo columnInfo = new ColumnInfo(colSet.getString("COLUMN_NAME"),colSet.getString("TYPE_NAME"));
                        cols.get(key).add(columnInfo);
                    }
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(),e);
        }
        return cols;
    }
}