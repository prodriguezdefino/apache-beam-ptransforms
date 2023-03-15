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

import static com.google.cloud.pso.beam.common.formats.AvroUtils.retrieveAvroSchemaFromLocation;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.STRING;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/** A format handler for {@link Transport} objects containing Avro {@link GenericRecord} data. */
public record AvroGenericRecordTransportHandler(AvroSchema schema)
    implements TransportFormats.Handler<GenericRecord> {

  record AvroSchema(Schema schema) {}

  public AvroGenericRecordTransportHandler(String avroSchemaLocation) {
    this(new AvroSchema(retrieveAvroSchemaFromLocation(avroSchemaLocation)));
  }

  @Override
  public byte[] encode(GenericRecord element) {
    try {
      var writer =
          new GenericDatumWriter<GenericRecord>(
              Optional.ofNullable(element.getSchema()).orElse(schema.schema()));
      var stream = new ByteArrayOutputStream();
      var encoder = EncoderFactory.get().binaryEncoder(stream, null);
      writer.write(element, encoder);
      encoder.flush();
      return stream.toByteArray();
    } catch (IOException ex) {
      throw new RuntimeException("Problems while trying to encode element.", ex);
    }
  }

  @Override
  public GenericRecord decode(byte[] encodedElement) {
    try {
      var reader = new GenericDatumReader<GenericRecord>(schema.schema());
      var avroRec = new GenericData.Record(schema.schema());
      var decoder =
          DecoderFactory.get().binaryDecoder(encodedElement, 0, encodedElement.length, null);
      reader.read(avroRec, decoder);
      return avroRec;
    } catch (IOException ex) {
      throw new RuntimeException("Problems while trying to decode element.", ex);
    }
  }

  private Schema.Field validateField(String propertyName) {
    return Optional.ofNullable(schema.schema().getField(propertyName))
        .orElseThrow(
            () -> new IllegalArgumentException("Field not found in schema: " + propertyName));
  }

  @Override
  public Long longValue(GenericRecord element, String propertyName) {
    var field = validateField(propertyName);
    return switch (field.schema().getType()) {
      case INT -> ((Integer) element.get(propertyName)).longValue();
      case LONG -> ((Long) element.get(propertyName));
      case DOUBLE -> ((Double) element.get(propertyName)).longValue();
      case FLOAT -> ((Float) element.get(propertyName)).longValue();
      default -> throw new IllegalArgumentException(
          "Computed field type is invalid (not numerical).");
    };
  }

  @Override
  public Double doubleValue(GenericRecord element, String propertyName) {
    var field = validateField(propertyName);
    return switch (field.schema().getType()) {
      case INT -> ((Integer) element.get(propertyName)).doubleValue();
      case LONG -> ((Long) element.get(propertyName)).doubleValue();
      case DOUBLE -> ((Double) element.get(propertyName));
      case FLOAT -> ((Float) element.get(propertyName)).doubleValue();
      default -> throw new IllegalArgumentException(
          "Computed field type is invalid (not numerical).");
    };
  }

  @Override
  public String stringValue(GenericRecord element, String propertyName) {
    var field = validateField(propertyName);
    return switch (field.schema().getType()) {
      case STRING -> (String) element.get(propertyName);
      default -> throw new IllegalArgumentException(
          "Computed field type is invalid (not a string).");
    };
  }
}
