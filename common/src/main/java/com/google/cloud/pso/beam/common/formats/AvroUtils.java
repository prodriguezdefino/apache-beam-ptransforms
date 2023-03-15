/*
 * Copyright (C) 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.pso.beam.common.formats;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.thrift.ThriftData;
import org.apache.avro.thrift.ThriftDatumWriter;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class AvroUtils {

  private static final Logger LOG = LoggerFactory.getLogger(AvroUtils.class);

  public static Schema retrieveAvroSchemaFromLocation(String avroSchemaLocation) {
    InputStream iStream = null;

    try {
      if (avroSchemaLocation.startsWith("classpath://")) {
        iStream =
            AvroUtils.class.getResourceAsStream(avroSchemaLocation.replace("classpath://", "/"));
      } else {
        iStream =
            Channels.newInputStream(
                FileSystems.open(FileSystems.matchNewResource(avroSchemaLocation, false)));
      }
      return new Schema.Parser().parse(iStream);
    } catch (Exception ex) {
      var msg =
          "Error while trying to retrieve the avro schema from location " + avroSchemaLocation;
      throw new RuntimeException(msg, ex);
    }
  }

  public static Schema retrieveAvroSchemaFromThriftClassName(String className) {
    return ThriftData.get().getSchema(ThriftUtils.retrieveThriftClass(className));
  }

  public static GenericRecord retrieveGenericRecordFromThriftData(
      TBase<?, ?> thriftObject, Schema avroSchema) {
    try {
      var bao = new ByteArrayOutputStream();
      var w = new ThriftDatumWriter<>(avroSchema);
      var e = EncoderFactory.get().binaryEncoder(bao, null);
      w.write(thriftObject, e);
      e.flush();
      return new GenericDatumReader<GenericRecord>(avroSchema)
          .read(
              null,
              DecoderFactory.get()
                  .binaryDecoder(new ByteArrayInputStream(bao.toByteArray()), null));
    } catch (IOException ex) {
      var msg = "Error while trying to retrieve a generic record from the thrift data.";
      LOG.error(msg, ex);
      throw new RuntimeException(msg, ex);
    }
  }
}
