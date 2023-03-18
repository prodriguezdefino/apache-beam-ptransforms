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

import autovalue.shaded.com.google.common.base.Preconditions;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.util.Arrays;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.sdk.io.FileSystems;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.Schema;
import org.everit.json.schema.StringSchema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.json.JSONTokener;

/** Collection of utility methods to use when processing JSON related types. */
public class JsonUtils {
  /**
   * Transform a JSON schema into its equivalent AVRO schema.
   *
   * @param jsonSchema a parsed JSON schema
   * @return An AVRO schema.
   * @throws IllegalArgumentException when the provided JSON schema is not supported.
   */
  public static org.apache.avro.Schema jsonSchemaToAvroSchema(Schema jsonSchema) {
    var avroSchemaBuilder = SchemaBuilder.builder();

    if (jsonSchema instanceof ReferenceSchema refSchema) {
      return jsonSchemaToAvroSchema(refSchema.getReferredSchema());
    } else if (jsonSchema instanceof ArraySchema arraySchema) {
      // we assume here that all the items have the same schema
      Preconditions.checkArgument(
          arraySchema.getItemSchemas().size() == 1,
          "There should be only 1 common schema for the array items: " + jsonSchema.toString());
      return avroSchemaBuilder
          .array()
          .items(jsonSchemaToAvroSchema(arraySchema.getContainedItemSchema()));
    } else if (jsonSchema instanceof ObjectSchema objSchema) {
      record ExtractedJsonSchemaPropertyInfo(
          String name,
          Boolean hasDefault,
          Object defaultValue,
          org.apache.avro.Schema avroSchema) {}

      var propsAndSchemas =
          objSchema.getPropertySchemas().entrySet().stream()
              .map(
                  entry ->
                      new ExtractedJsonSchemaPropertyInfo(
                          entry.getKey(),
                          entry.getValue().hasDefaultValue(),
                          entry.getValue().getDefaultValue(),
                          jsonSchemaToAvroSchema(entry.getValue())))
              .toList();
      var location = jsonSchema.getSchemaLocation().replace("#/", "").replace("/", ".");
      var namespace = location;
      var name =
          Arrays.stream(location.split("\\.")).reduce((first, last) -> last).orElse(location);
      var recordSchemaFields = avroSchemaBuilder.record(name).namespace(namespace).fields();
      for (var nameAndSchema : propsAndSchemas) {
        var partialField =
            recordSchemaFields.name(nameAndSchema.name()).type(nameAndSchema.avroSchema());
        if (nameAndSchema.hasDefault()) partialField.withDefault(nameAndSchema.defaultValue());
        else partialField.noDefault();
      }
      return recordSchemaFields.endRecord();
    } else if (jsonSchema instanceof CombinedSchema combSchema) {
      // we support only null + other simple schema here, if this definition has more, then we fail
      // the translation
      if (combSchema.getSubschemas().size() > 2)
        throw new IllegalArgumentException(
            "The provided JSON schema has more than one option for the field "
                + "(besides the null option) and is not supported: "
                + combSchema.toString());
      var hasNullSchema =
          combSchema.getSubschemas().stream().filter(schema -> schema instanceof NullSchema).count()
              == 1;
      if (!hasNullSchema)
        throw new IllegalArgumentException(
            "The provided JSON schema does not have a null schema as part of the combined schema, "
                + "that's not supported: "
                + combSchema.toString());
      return avroSchemaBuilder
          .unionOf()
          .nullType()
          .and()
          .type(
              jsonSchemaToAvroSchema(
                  combSchema.getSubschemas().stream()
                      .filter(schema -> !(schema instanceof NullSchema))
                      .findFirst()
                      .orElseThrow(
                          () ->
                              new IllegalArgumentException(
                                  "The provided JSON schema should have at least a non null schema as part "
                                      + "of the combined schema: "
                                      + combSchema.toString()))))
          .endUnion();
    } else if (jsonSchema instanceof EnumSchema enumSchema) {
      return avroSchemaBuilder
          .enumeration(enumSchema.getTitle())
          .symbols(
              enumSchema.getPossibleValues().stream().map(Object::toString).toArray(String[]::new));
    } else if (jsonSchema instanceof NumberSchema numSchema) {
      if (numSchema.requiresInteger()) {
        if (shouldSchemaUseLongInsteadOfInteger(numSchema)) {
          return avroSchemaBuilder.longType();
        } else return avroSchemaBuilder.intType();
      } else return avroSchemaBuilder.doubleType();
    } else if (jsonSchema instanceof BooleanSchema) {
      return avroSchemaBuilder.booleanType();
    } else if (jsonSchema instanceof StringSchema) {
      return avroSchemaBuilder.stringType();
    } else
      throw new IllegalArgumentException("JSON schema not supported: " + jsonSchema.toString());
  }

  public static boolean shouldSchemaUseLongInsteadOfInteger(NumberSchema numSchema) {
    if (compareAgainstMaxInt(numSchema.getMaximum()) < 0) {
      return true;
    } else return false;
  }

  static Integer compareAgainstMaxInt(Number maxValue) {
    return BigDecimal.valueOf(Integer.MAX_VALUE)
        .compareTo(BigDecimal.valueOf(maxValue.doubleValue()));
  }

  public static Schema retrieveJsonSchemaFromLocation(String jsonSchemaLocation) {
    try {
      if (jsonSchemaLocation.startsWith("classpath://")) {
        try (InputStream inputStream =
            JsonUtils.class.getResourceAsStream(jsonSchemaLocation.replace("classpath://", "/"))) {
          return SchemaLoader.load(new JSONObject(new JSONTokener(inputStream)));
        }
      } else
        try (InputStream inputStream =
            Channels.newInputStream(
                FileSystems.open(FileSystems.matchNewResource(jsonSchemaLocation, false)))) {
          return SchemaLoader.load(new JSONObject(new JSONTokener(inputStream)));
        }
    } catch (Exception ex) {
      var msg =
          "Error while trying to retrieve the json schema from location " + jsonSchemaLocation;
      throw new RuntimeException(msg, ex);
    }
  }
}
