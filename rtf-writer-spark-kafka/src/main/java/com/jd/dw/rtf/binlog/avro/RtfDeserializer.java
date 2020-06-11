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

package com.jd.dw.rtf.binlog.avro;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.serialization.Deserializer;

public class RtfDeserializer implements Deserializer<RtfData> {

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {}

    @Override
    public RtfData deserialize(String topic, byte[] data) {
        if(data == null) {
            return null;
        }
        RtfData rtfData = new RtfData();
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        DatumReader<RtfData> userDatumReader = new SpecificDatumReader<>(rtfData.getSchema());
        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);
        try {
            rtfData = userDatumReader.read(null, decoder);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rtfData;
    }
}