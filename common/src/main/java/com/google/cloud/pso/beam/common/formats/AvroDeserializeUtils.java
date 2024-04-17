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

import static com.google.cloud.pso.beam.common.formats.json.JsonValueExtractors.*;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.values.Row;

/** */
public class AvroDeserializeUtils {

  private static final ImmutableMap<Schema.Type, ValueExtractor<?>> JSON_VALUE_GETTERS =
      ImmutableMap.<Schema.Type, ValueExtractor<?>>builder()
          .put(Schema.Type.BYTES, byteValueExtractor())
          .put(Schema.Type.INT, intValueExtractor())
          .put(Schema.Type.LONG, longValueExtractor())
          .put(Schema.Type.FLOAT, floatValueExtractor())
          .put(Schema.Type.DOUBLE, doubleValueExtractor())
          .put(Schema.Type.BOOLEAN, booleanValueExtractor())
          .put(Schema.Type.STRING, stringValueExtractor())
          .build();

  static final ImmutableMap<String, BiFunction<LogicalType, JsonNode, Object>>
      LOGICAL_TYPE_DECODERS =
          ImmutableMap.<String, BiFunction<LogicalType, JsonNode, Object>>builder()
              .put(
                  LogicalTypes.date().getName(),
                  (logicalType, value) -> dateValueExtractor().extractValue(value))
              .put(
                  LogicalTypes.decimal(1).getName(),
                  (logicalType, value) -> decimalValueExtractor().extractValue(value))
              .put(
                  LogicalTypes.timestampMicros().getName(),
                  (logicalType, value) -> datetimeValueExtractor().extractValue(value))
              .put(
                  LogicalTypes.timestampMillis().getName(),
                  (logicalType, value) -> datetimeValueExtractor().extractValue(value))
              .put(
                  LogicalTypes.uuid().getName(),
                  (logicalType, value) ->
                      UUID.fromString(stringValueExtractor().extractValue(value)))
              .build();

  private static final ImmutableSet<Schema.Type> SUPPORTED_TYPES = JSON_VALUE_GETTERS.keySet();
  private static final ImmutableSet<String> KNOWN_LOGICAL_TYPE_IDENTIFIERS =
      LOGICAL_TYPE_DECODERS.keySet();

  public static ObjectMapper newObjectMapperWith(GenericRecordJsonDeserializer deserializer) {
    SimpleModule module = new SimpleModule("genericRecordDeserializationModule");
    module.addDeserializer(GenericRecord.class, deserializer);

    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(module);

    return objectMapper;
  }

  public static GenericRecord jsonToGenericRecord(ObjectMapper objectMapper, String jsonString) {
    try {
      return objectMapper.readValue(jsonString, GenericRecord.class);
    } catch (JsonParseException | JsonMappingException jsonException) {
      throw new UnsupportedJsonExtractionException("Unable to parse Row", jsonException);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse json object: " + jsonString, e);
    }
  }

  public static GenericRecord jsonToGenericRecord(Schema avroSchema, String jsonString) {
    return jsonToGenericRecord(
        newObjectMapperWith(GenericRecordJsonDeserializer.forSchema(avroSchema)), jsonString);
  }

  public static void verifySchemaSupported(Schema schema) {
    ImmutableList<UnsupportedField> unsupportedFields = findUnsupportedFields(schema);
    if (!unsupportedFields.isEmpty()) {
      throw new UnsupportedJsonExtractionException(
          String.format(
              "Field type%s %s not supported when converting between JSON and Rows. Supported types are: %s",
              unsupportedFields.size() > 1 ? "s" : "",
              unsupportedFields.toString(),
              SUPPORTED_TYPES.toString()));
    }
  }

  private static class UnsupportedField {
    final String descriptor;
    final String type;

    UnsupportedField(String descriptor, String typeName) {
      this.descriptor = descriptor;
      this.type = typeName;
    }

    @Override
    public String toString() {
      return this.descriptor + "=" + this.type;
    }
  }

  private static ImmutableList<UnsupportedField> findUnsupportedFields(Schema schema) {
    return schema.getFields().stream()
        .flatMap((field) -> findUnsupportedFields(field).stream())
        .collect(ImmutableList.toImmutableList());
  }

  private static ImmutableList<UnsupportedField> findUnsupportedFields(Schema.Field field) {
    return findUnsupportedFields(field.schema(), field.name());
  }

  private static ImmutableList<UnsupportedField> findUnsupportedFields(
      Schema schema, String fieldName) {
    if (schema.hasFields()) {
      return schema.getFields().stream()
          .flatMap(
              (field) ->
                  findUnsupportedFields(field.schema(), fieldName + "." + field.name()).stream())
          .collect(ImmutableList.toImmutableList());
    }

    if (schema.getElementType() != null) {
      return findUnsupportedFields(schema.getElementType(), fieldName + "[]");
    }

    if (schema.getLogicalType() != null) {
      if (KNOWN_LOGICAL_TYPE_IDENTIFIERS.contains(schema.getLogicalType().getName())) {
        return ImmutableList.of();
      } else {
        return ImmutableList.of(new UnsupportedField(fieldName, schema.getLogicalType().getName()));
      }
    }

    if (!SUPPORTED_TYPES.contains(schema.getType())) {
      return ImmutableList.of(new UnsupportedField(fieldName, schema.getType().getName()));
    }

    return ImmutableList.of();
  }

  /** Jackson deserializer for parsing JSON into Avro {@link GenericRecord}. */
  public static class GenericRecordJsonDeserializer extends StdDeserializer<GenericRecord> {

    private static final boolean SEQUENTIAL = false;

    /**
     * An enumeration type for specifying how {@link GenericRecordJsonDeserializer} should expect
     * null values to be represented.
     *
     * <p>For example, when parsing JSON for the Schema {@code (str: REQUIRED STRING, int: NULLABLE
     * INT64)}:
     *
     * <ul>
     *   <li>If configured with {@code REQUIRE_NULL}, {@code {"str": "foo", "int": null}} would be
     *       accepted.
     *   <li>If configured with {@code REQUIRE_MISSING}, {@code {"str": "bar"}} would be accepted,
     *       and would yield a {@link Row} with {@code null} for the {@code int} field.
     *   <li>If configured with {@code ALLOW_MISSING_OR_NULL}, either JSON string would be accepted.
     * </ul>
     */
    public enum NullBehavior {
      /**
       * Specifies that a null value may be represented as either a missing field or a null value in
       * the input JSON.
       */
      ACCEPT_MISSING_OR_NULL,
      /**
       * Specifies that a null value must be represented with a null value in JSON. If the field is
       * missing an {@link UnsupportedGenericRecordJsonException} will be thrown.
       */
      REQUIRE_NULL,
      /**
       * Specifies that a null value must be represented with a missing field in JSON. If the field
       * has a null value an {@link UnsupportedGenericRecordJsonException} will be thrown.
       */
      REQUIRE_MISSING,
    }

    private final Schema schema;
    private NullBehavior nullBehavior = NullBehavior.ACCEPT_MISSING_OR_NULL;

    /** Creates a deserializer for a {@link Row} {@link Schema}. */
    public static GenericRecordJsonDeserializer forSchema(Schema schema) {
      verifySchemaSupported(schema);
      return new GenericRecordJsonDeserializer(schema);
    }

    private GenericRecordJsonDeserializer(Schema schema) {
      super(GenericRecord.class);
      this.schema = schema;
    }

    /**
     * Sets the behaviour of the deserializer when retrieving null values in the input JSON. See
     * {@link NullBehavior} for a description of the options. Default value is {@code
     * ACCEPT_MISSING_OR_NULL}.
     */
    public GenericRecordJsonDeserializer withNullBehavior(NullBehavior behavior) {
      this.nullBehavior = behavior;
      return this;
    }

    @Override
    public GenericRecord deserialize(
        JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {

      // Parse and convert the root object to Row as if it's a nested field with name 'root'
      return (GenericRecord)
          extractJsonNodeValue(FieldValue.of("root", schema, jsonParser.readValueAsTree()));
    }

    private Object extractJsonNodeValue(FieldValue fieldValue) {
      if (fieldValue.schema().isNullable()) {
        if (!fieldValue.isJsonValuePresent()) {
          switch (this.nullBehavior) {
            case ACCEPT_MISSING_OR_NULL:
            case REQUIRE_MISSING:
              return null;
            case REQUIRE_NULL:
              throw new UnsupportedJsonExtractionException(
                  "Field '" + fieldValue.name() + "' is not present in the JSON object.");
          }
        }

        if (fieldValue.isJsonNull()) {
          switch (this.nullBehavior) {
            case ACCEPT_MISSING_OR_NULL:
            case REQUIRE_NULL:
              return null;
            case REQUIRE_MISSING:
              throw new UnsupportedJsonExtractionException(
                  "Field '" + fieldValue.name() + "' has a null value in the JSON object.");
          }
        }
      } else {
        // field is not nullable
        if (!fieldValue.isJsonValuePresent()) {
          throw new UnsupportedJsonExtractionException(
              "Non-nullable field '" + fieldValue.name() + "' is not present in the JSON object.");
        } else if (fieldValue.isJsonNull()) {
          throw new UnsupportedJsonExtractionException(
              "Non-nullable field '" + fieldValue.name() + "' has value null in the JSON object.");
        }
      }

      if (fieldValue.isRowType()) {
        return jsonObjectToRecord(fieldValue);
      }

      if (fieldValue.isArrayType()) {
        return jsonArrayToList(fieldValue);
      }

      if (fieldValue.isLogicalType()) {
        LogicalType logicalType = LogicalTypes.fromSchema(fieldValue.schema());
        String logicalTypeName = logicalType.getName();
        BiFunction<LogicalType, JsonNode, Object> logicalTypeDecoder =
            LOGICAL_TYPE_DECODERS.get(logicalTypeName);
        if (logicalTypeDecoder == null) {
          throw new IllegalArgumentException("Unsupported logical type " + logicalTypeName);
        }
        return logicalTypeDecoder.apply(logicalType, fieldValue.jsonValue());
      }

      return extractJsonPrimitiveValue(fieldValue);
    }

    private GenericRecord jsonObjectToRecord(FieldValue rowFieldValue) {
      if (!rowFieldValue.isJsonObject()) {
        throw new UnsupportedJsonExtractionException(
            "Expected JSON object for field '"
                + rowFieldValue.name()
                + "'. Unable to convert '"
                + rowFieldValue.jsonValue().asText()
                + "' to Beam Row, it is not a JSON object. Currently only JSON objects can be parsed to Beam Rows");
      }

      GenericRecordBuilder recordBuilder = new GenericRecordBuilder(rowFieldValue.schema());

      for (Schema.Field field : rowFieldValue.schema().getFields()) {
        recordBuilder.set(
            field,
            extractJsonNodeValue(
                FieldValue.of(
                    field.name(), field.schema(), rowFieldValue.jsonFieldValue(field.name()))));
      }
      return recordBuilder.build();
    }

    private Object jsonArrayToList(FieldValue arrayFieldValue) {
      if (!arrayFieldValue.isJsonArray()) {
        throw new UnsupportedJsonExtractionException(
            "Expected JSON array for field '"
                + arrayFieldValue.name()
                + "'. Instead got "
                + arrayFieldValue.jsonNodeType().name());
      }

      return arrayFieldValue
          .jsonArrayElements()
          .map(
              jsonArrayElement ->
                  extractJsonNodeValue(
                      FieldValue.of(
                          arrayFieldValue.name() + "[]",
                          arrayFieldValue.arrayElementSchema(),
                          jsonArrayElement)))
          .collect(ImmutableList.toImmutableList());
    }

    private static Object extractJsonPrimitiveValue(FieldValue fieldValue) {
      try {
        return JSON_VALUE_GETTERS
            .get(fieldValue.schema().getType())
            .extractValue(fieldValue.jsonValue());
      } catch (RuntimeException e) {
        throw new UnsupportedJsonExtractionException(
            "Unable to get value from field '"
                + fieldValue.name()
                + "'. Schema type '"
                + fieldValue.typeName()
                + "'. JSON node type "
                + fieldValue.jsonNodeType().name(),
            e);
      }
    }

    /**
     * Helper class to keep track of schema field type, name, and actual json value for the field.
     */
    @AutoValue
    abstract static class FieldValue {
      abstract String name();

      abstract Schema schema();

      abstract @Nullable JsonNode jsonValue();

      String typeName() {
        return schema().getName();
      }

      boolean isJsonValuePresent() {
        return jsonValue() != null;
      }

      boolean isJsonNull() {
        return jsonValue().isNull();
      }

      JsonNodeType jsonNodeType() {
        return jsonValue().getNodeType();
      }

      boolean isJsonArray() {
        return jsonValue().isArray();
      }

      Stream<JsonNode> jsonArrayElements() {
        return StreamSupport.stream(jsonValue().spliterator(), SEQUENTIAL);
      }

      boolean isArrayType() {
        return Schema.Type.ARRAY.equals(schema().getType());
      }

      private boolean isLogicalType() {
        LogicalType logicalType = LogicalTypes.fromSchema(schema());
        return logicalType != null;
      }

      Schema arrayElementSchema() {
        return schema().getElementType();
      }

      boolean isJsonObject() {
        return jsonValue().isObject();
      }

      JsonNode jsonFieldValue(String fieldName) {
        return jsonValue().get(fieldName);
      }

      boolean isRowType() {
        return Schema.Type.RECORD.equals(schema().getType());
      }

      static FieldValue of(String name, Schema schema, JsonNode jsonValue) {
        return new AutoValue_AvroDeserializeUtils_GenericRecordJsonDeserializer_FieldValue(
            name, schema, jsonValue);
      }
    }
  }
}
