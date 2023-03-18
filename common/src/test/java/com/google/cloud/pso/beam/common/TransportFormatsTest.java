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
package com.google.cloud.pso.beam.common;

import com.google.cloud.pso.beam.common.formats.AggregationResultValue;
import com.google.cloud.pso.beam.common.formats.JsonUtils;
import com.google.cloud.pso.beam.common.formats.TransportFormats;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.values.KV;
import org.everit.json.schema.ValidationException;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;

/** */
public class TransportFormatsTest {
  static Random RANDOM = new Random();

  AggregationResultValue randomResultValue() {
    return new AggregationResultValue(
        RandomStringUtils.randomAlphabetic(10)
            + "#"
            + RandomStringUtils.randomNumeric(3)
            + "#"
            + RandomStringUtils.randomAlphabetic(6),
        IntStream.range(0, RANDOM.nextInt(3))
            .mapToObj(i -> KV.of(RandomStringUtils.randomAlphabetic(5), i))
            .collect(Collectors.toMap(KV::getKey, KV::getValue)));
  }

  @Test
  public void testAggregationValueSerializationConsistent() {
    var handler =
        TransportFormats.<AggregationResultValue>handlerFactory(
                TransportFormats.Format.AGGREGATION_RESULT)
            .apply(null);
    var original = randomResultValue();
    var encoded = handler.encode(original);
    var decoded = handler.decode(encoded);

    Assert.assertEquals(original, decoded);
  }

  @Test
  public void testAggregationValueSerializationMultiThreadedExecution() {
    var handler =
        TransportFormats.<AggregationResultValue>handlerFactory(
                TransportFormats.Format.AGGREGATION_RESULT)
            .apply(null);

    IntStream.range(0, 1000)
        .parallel()
        .mapToObj(i -> randomResultValue())
        .map(r -> KV.of(r, handler.encode(r)))
        .map(kvEnc -> KV.of(kvEnc.getKey(), handler.decode(kvEnc.getValue())))
        .toList()
        .forEach(kv -> Assert.assertEquals(kv.getKey(), kv.getValue()));
  }

  private static final String MESSAGE_CONTENT = "a dummy message content";
  private static final Long STARTUP_CONTENT = 10002222L;
  private static final String VALID_JSON_STRING_UNFORMATTED =
      """
      {
        "topic" : {
                  "id" : "topic id",
                  "value" : 2,
                  "name" : "topic name"
                },
        "message" : "%1$s",
        "user" : {
          "location" : "some-location",
          "startup" : %2$d,
          "description" : "some description as well",
          "uuid" : "some uuid"
        }
      }
      """;
  private static final String VALID_JSON_STRING =
      String.format(VALID_JSON_STRING_UNFORMATTED, MESSAGE_CONTENT, STARTUP_CONTENT);

  private static final String INVALID_JSON_STRING =
      """
      {
          "startup" : 10002222,
          "description" : "some location" ,
          "location" : "some-location" ,
          "uuid" : "some uuid"
      }
      """;

  @Test(expected = ValidationException.class)
  public void testJsonSchemaValidationFails() throws IOException {
    var schema = JsonUtils.retrieveJsonSchemaFromLocation("classpath://json/message-schema.json");
    schema.validate(new JSONObject(INVALID_JSON_STRING));
  }

  @Test
  public void testJsonSchemaValidation() throws IOException {
    var schema = JsonUtils.retrieveJsonSchemaFromLocation("classpath://json/message-schema.json");
    schema.validate(new JSONObject(VALID_JSON_STRING));
  }

  @Test
  public void testJsonSchemaToAvroSchema() throws IOException {
    var schema = JsonUtils.retrieveJsonSchemaFromLocation("classpath://json/message-schema.json");
    var avroSchema = JsonUtils.jsonSchemaToAvroSchema(schema);
    try (var inputStream =
        TransportFormatsTest.class
            .getClassLoader()
            .getResourceAsStream("avro/message-schema.avro")) {
      var expectedAvroSchema = new org.apache.avro.Schema.Parser().parse(inputStream);
      System.err.println("generated: " + avroSchema.toString(true));
      System.err.println("expected: " + expectedAvroSchema.toString(true));
      Assert.assertTrue(avroSchema.equals(expectedAvroSchema));
    }
  }

  @Test
  public void testJsonEncodeDecode() {
    var handlerFactory = TransportFormats.handlerFactory(TransportFormats.Format.JSON);
    var handler = handlerFactory.<JSONObject>apply("classpath://json/message-schema.json");
    var json = new JSONObject(VALID_JSON_STRING); 
    var bytes = handler.encode(json);
    var decoded = handler.decode(bytes);
    Assert.assertEquals(json.toString(), decoded.toString());
  }

  @Test
  public void testJsonStringDataExtraction() throws Exception {
    var handlerFactory = TransportFormats.handlerFactory(TransportFormats.Format.JSON);
    var handler = handlerFactory.<JSONObject>apply("classpath://json/message-schema.json");
    var propertyName = "message";
    var json = new JSONObject(VALID_JSON_STRING);
    var retrieved = handler.stringValue(json, propertyName);
    Assert.assertEquals(MESSAGE_CONTENT, retrieved);
  }

  @Test
  public void testJsonIntegerDataExtraction() throws Exception {
    var handlerFactory = TransportFormats.handlerFactory(TransportFormats.Format.JSON);
    var handler = handlerFactory.<JSONObject>apply("classpath://json/message-schema.json");
    var propertyName = "user.startup";
    var json = new JSONObject(VALID_JSON_STRING);
    var retrieved = handler.longValue(json, propertyName);
    Assert.assertEquals(STARTUP_CONTENT, retrieved);
  }

  @Test
  public void testJsonSchemaToTableSchema() throws IOException {

    try (var inputStream =
        TransportFormatsTest.class
            .getClassLoader()
            .getResourceAsStream("avro/message-schema.avro")) {
      var avroSchema = new org.apache.avro.Schema.Parser().parse(inputStream);
      var tableSchema = BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(avroSchema));
      System.err.println("print table schema: " + tableSchema.toString());
      System.err.println("print json table schema: " + BigQueryHelpers.toJsonString(tableSchema));
      var json = new JSONObject(INVALID_JSON_STRING);
      var jsonBytes = json.toString().getBytes(StandardCharsets.UTF_8);
      var bais = new ByteArrayInputStream(jsonBytes);
      @SuppressWarnings("deprecation")
      var tablerRow = TableRowJsonCoder.of().decode(bais, Coder.Context.OUTER);
      System.err.println("table row: " + tablerRow.toPrettyString());
      System.err.println("input json: " + INVALID_JSON_STRING);
    }
  }
}
