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

package com.jd.dw.rtf.binlog.model;


public class TableInfo {

    private String databaseName;
    private String tableName;
    private String fullName;

    @Override
    public boolean equals(Object o){
        if(this == o) {
            return true;
        }
        if(o == null || this.getClass()!=o.getClass()) {
            return false;
        }
        TableInfo tableInfo = (TableInfo)o;
        if(!this.databaseName.equals(tableInfo.getDatabaseName())) {
            return false;
        }
        if(!this.tableName.equals(tableInfo.getTableName())) {
            return false;
        }
        if(!this.fullName.equals(tableInfo.getFullName())) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode(){
        int result = this.tableName.hashCode();
        result = 31*result+this.databaseName.hashCode();
        result = 31*result+this.fullName.hashCode();
        return result;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }
}