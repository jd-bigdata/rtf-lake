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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.jd.dw.rtf.binlog.avro.RtfData;

public class RtfSerializer implements Serializer<RtfData> {

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> arg0, boolean arg1) {}

    @Override
    public byte[] serialize(String topic, RtfData data) {
        if(data == null) {
            return null;
        }
        DatumWriter<RtfData> writer = new SpecificDatumWriter<>(data.getSchema());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        try {
            writer.write(data, encoder);
        }catch (IOException e) {
            throw new SerializationException(e.getMessage());
        }
        return out.toByteArray();
    }

}