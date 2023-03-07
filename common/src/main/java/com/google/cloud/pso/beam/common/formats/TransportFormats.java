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

import static com.google.cloud.pso.beam.common.formats.AvroUtils.*;
import static com.google.cloud.pso.beam.common.formats.ThriftUtils.*;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;

import com.google.common.collect.Maps;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TTransportException;

/**
 * Defines the supported formats, encoding and decoding functionalities and data extraction methods.
 */
public class TransportFormats {

  static final Map<String, Handler> HANDLERS = Maps.newConcurrentMap();

  public enum Format {
    THRIFT,
    AVRO,
    AGGREGATION_RESULT
  }

  public static Function<String, Handler> handlerFactory(Format format) {
    return switch (format) {
      case THRIFT -> className ->
          HANDLERS.computeIfAbsent(className, key -> new ThriftHandler(key));
      case AVRO -> avroSchemaLocation ->
          HANDLERS.computeIfAbsent(avroSchemaLocation, key -> new AvroGenericRecordHandler(key));
      case AGGREGATION_RESULT -> dummy ->
          HANDLERS.computeIfAbsent(
              Format.AGGREGATION_RESULT.name(), key -> new AggregationResultValueHandler());
    };
  }

  public sealed interface Handler<T>
      permits ThriftHandler, AvroGenericRecordHandler, AggregationResultValueHandler {

    byte[] encode(T element);

    T decode(byte[] encodedElement);

    Long longValue(T element, String propertyName);

    Double doubleValue(T element, String propertyName);

    String stringValue(T element, String propertyName);
  }

  public record AggregationResultValueHandler()
      implements Handler<Map<String, Map<String, Number>>>, Serializable {

    @Override
    public byte[] encode(Map<String, Map<String, Number>> element) {
      try {
        var baos = new ByteArrayOutputStream();
        try (var objectOutputStream = new ObjectOutputStream(baos)) {
          objectOutputStream.writeObject(element);
          objectOutputStream.flush();
        }
        return baos.toByteArray();
      } catch (IOException ex) {
        throw new IllegalArgumentException(
            "The serialization of the provided element was not possible.", ex);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Map<String, Number>> decode(byte[] encodedElement) {
      try (var ois = new ObjectInputStream(new ByteArrayInputStream(encodedElement)); ) {
        return (Map<String, Map<String, Number>>) ois.readObject();
      } catch (IOException | ClassNotFoundException ex) {
        throw new IllegalArgumentException(
            "The provided byte array did not conform the expected structure.", ex);
      }
    }

    String validateAndRetrieveKey(Map<String, Map<String, Number>> map) {
      // outer map should only contain 1 entry as aggregation field as key
      return map.keySet().stream()
          .findFirst()
          .orElseThrow(
              () ->
                  new IllegalArgumentException(
                      "The provided element ("
                          + map
                          + ") does not have the expected structure. "
                          + "This value handler assumes the encoded value as an aggregation "
                          + "result structure map<aggregation-key-field, <result, value>>."));
    }

    @Override
    public Long longValue(Map<String, Map<String, Number>> element, String propertyName) {
      var storedKey = validateAndRetrieveKey(element);
      return element.get(storedKey).get(propertyName).longValue();
    }

    @Override
    public Double doubleValue(Map<String, Map<String, Number>> element, String propertyName) {
      var storedKey = validateAndRetrieveKey(element);
      return element.get(storedKey).get(propertyName).doubleValue();
    }

    /**
     * Assumes is used to retrieve the aggregation's key. It discards previously computed
     * aggregation key components (since this is coming from a previous aggregation they keys will
     * take the form [field#field-value#result-info].
     *
     * @param element the decoded map for the aggregation result.
     * @param propertyName the key property name
     * @return the aggregation key value.
     */
    @Override
    public String stringValue(Map<String, Map<String, Number>> element, String propertyName) {
      // outer map should only contain 1 entry as aggregation field as key
      var storedKey = element.keySet().stream().findFirst();
      return storedKey
          .filter(k -> k.contains(propertyName))
          // remove the property from the aggregation key and then discard previously computed
          // aggregation key components from key
          .map(k -> k.replace(propertyName + "#", "").split("#")[0])
          .orElseThrow(
              () ->
                  new IllegalArgumentException(
                      "The property provided ("
                          + propertyName
                          + ") does not match the key field stored in the value: "
                          + storedKey
                          + ". This value handler assumes the encoded value as an aggregation "
                          + "result structure map<aggregation-key-field, <result, value>>."));
    }
  }

  public record ThriftHandler(Format format, ThriftClass thriftClass)
      implements Handler<TBase>, Serializable {

    record ThriftClass(Class<? extends TBase> clazz, Class<? extends TFieldIdEnum> fieldEnum)
        implements Serializable {

      TFieldIdEnum retrieveFieldEnumValueByFieldName(String fieldName) {
        try {
          var method = fieldEnum.getMethod("findByName", String.class);
          return (TFieldIdEnum)
              Optional.ofNullable(method.invoke(null, fieldName))
                  .orElseThrow(
                      () ->
                          new IllegalArgumentException(
                              "Error while retrieving the field id enum with field name: "
                                  + fieldName));
        } catch (NoSuchMethodException
            | SecurityException
            | IllegalAccessException
            | InvocationTargetException ex) {
          throw new RuntimeException(
              "Problems while trying to retrieve the enum with field value name: " + fieldName, ex);
        }
      }
    }

    public ThriftHandler(String thriftClassName) {
      this(
          Format.THRIFT,
          new ThriftClass(
              retrieveThriftClass(thriftClassName), retrieveThirftFieldEnum(thriftClassName)));
    }

    @Override
    public byte[] encode(TBase element) {
      try {
        var serializer = new TSerializer(new TBinaryProtocol.Factory());
        return serializer.serialize(element);
      } catch (TTransportException e) {
        throw new RuntimeException("Error while creating a TSerializer.", e);
      } catch (TException e) {
        throw new RuntimeException("Error while serializing the object.", e);
      }
    }

    @Override
    public TBase decode(byte[] encodedElement) {
      try {
        var thriftEmptyInstance = thriftClass.clazz().getConstructor().newInstance();
        return getThriftObjectFromData(thriftEmptyInstance, encodedElement);
      } catch (Exception ex) {
        throw new RuntimeException(
            "Error while trying to decode binary data for thrift class " + thriftClass.toString(),
            ex);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Long longValue(TBase element, String propertyName) {
      var field = validateFieldAndReturn(element, propertyName);
      return switch (field.valueMetaData.type) {
        case TType.I16 -> ((Short)
                element.getFieldValue(
                    thriftClass().retrieveFieldEnumValueByFieldName(propertyName)))
            .longValue();
        case TType.I32 -> ((Integer)
                element.getFieldValue(
                    thriftClass().retrieveFieldEnumValueByFieldName(propertyName)))
            .longValue();
        case TType.I64 -> ((Long)
            element.getFieldValue(thriftClass().retrieveFieldEnumValueByFieldName(propertyName)));
        case TType.DOUBLE -> ((Double)
                element.getFieldValue(
                    thriftClass().retrieveFieldEnumValueByFieldName(propertyName)))
            .longValue();
        default -> throw new IllegalArgumentException(
            "Computed field type is invalid (not numerical).");
      };
    }

    @SuppressWarnings("unchecked")
    @Override
    public Double doubleValue(TBase element, String propertyName) {
      var field = validateFieldAndReturn(element, propertyName);
      return switch (field.valueMetaData.type) {
        case TType.I16 -> ((Short)
                element.getFieldValue(
                    thriftClass().retrieveFieldEnumValueByFieldName(propertyName)))
            .doubleValue();
        case TType.I32 -> ((Integer)
                element.getFieldValue(
                    thriftClass().retrieveFieldEnumValueByFieldName(propertyName)))
            .doubleValue();
        case TType.I64 -> ((Long)
                element.getFieldValue(
                    thriftClass().retrieveFieldEnumValueByFieldName(propertyName)))
            .doubleValue();
        case TType.DOUBLE -> ((Double)
            element.getFieldValue(thriftClass().retrieveFieldEnumValueByFieldName(propertyName)));
        default -> throw new IllegalArgumentException(
            "Computed field type is invalid (not numerical).");
      };
    }

    @SuppressWarnings("unchecked")
    @Override
    public String stringValue(TBase element, String propertyName) {
      var field = validateFieldAndReturn(element, propertyName);
      return switch (field.valueMetaData.type) {
        case TType.STRING -> ((String)
            element.getFieldValue(thriftClass().retrieveFieldEnumValueByFieldName(propertyName)));
        default -> throw new IllegalArgumentException(
            "Computed field type is invalid (not a string).");
      };
    }
  }

  public record AvroGenericRecordHandler(Format format, AvroSchema schema)
      implements Handler<GenericRecord> {

    record AvroSchema(Schema schema) {}

    public AvroGenericRecordHandler(String avroSchemaLocation) {
      this(Format.AVRO, new AvroSchema(retrieveAvroSchemaFromLocation(avroSchemaLocation)));
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
}
