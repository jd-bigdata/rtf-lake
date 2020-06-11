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

package com.jd.dw.rtf.writer.partition;

import com.jd.dw.rtf.writer.Constants;

public class FilePartition {
    private String filename;
    private long startKey;
    private long endKey;

    public FilePartition(String filename) {//.rtf.0
        this.filename = filename;
        filename = filename.replace(Constants.RTF_FILE_PREFIX, "");
        String[] filenames = filename.split(Constants.STRIKETHROUGH);
        this.startKey = Long.parseLong(filenames[0]);
        this.endKey = Long.parseLong(filenames[1]);
    }

    public String getFilename() {
        return this.filename;
    }

    public boolean isKeyInFile(int key) {
        if ((key >= this.startKey) && (key <= this.endKey)) {
            return true;
        }
        return false;
    }
}