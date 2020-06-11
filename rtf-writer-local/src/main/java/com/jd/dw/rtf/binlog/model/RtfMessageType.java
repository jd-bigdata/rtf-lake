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


import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RtfMessageType {
    private long mid = 0;//message id
    private String db = null;
    private String tb = null;
    private long ts = 0;//Timestamp of operation
    private String opt = null;//insert delete update
    private Map<String, String> src = null;
    private Map<String, String> cur = null;

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public long getMid() {
        return mid;
    }

    public void setMid(long mid) {
        this.mid = mid;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTb() {
        return tb;
    }

    public void setTb(String tb) {
        this.tb = tb;
    }


    public String getOpt() {
        return opt;
    }

    public void setOpt(String opt) {
        this.opt = opt;
    }


    public Map<String, String> getSrc() {
        return src;
    }

    public void setSrc(Map<String, String> src) {
        this.src = src;
    }

    public Map<String, String> getCur() {
        return cur;
    }

    public void setCur(Map<String, String> cur) {
        this.cur = cur;
    }

    private static AtomicLong uuid = new AtomicLong(0);

    public RtfMessageType() {
    }

    public RtfMessageType(final Event event, String databaseName, String tableName) {
        this.init(event);
        this.db = databaseName;
        this.tb = tableName;
    }

    private void init(final Event event) {
        this.mid = uuid.getAndAdd(1);
        EventHeader eventHeader = event.getHeader();
        this.ts = eventHeader.getTimestamp();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();

        builder.append("{mid:").append(mid);
        builder.append(",db:").append(db);
        builder.append(",tb:").append(tb);
        builder.append(",ts:").append(ts);
        builder.append(",opt:").append(opt);
        builder.append(",src:").append(src);
        builder.append(",cur:").append(cur).append("}");


        return builder.toString();

    }
}