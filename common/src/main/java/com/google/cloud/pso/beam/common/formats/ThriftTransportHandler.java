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

import static com.google.cloud.pso.beam.common.formats.ThriftUtils.getThriftObjectFromData;
import static com.google.cloud.pso.beam.common.formats.ThriftUtils.retrieveThirftFieldEnum;
import static com.google.cloud.pso.beam.common.formats.ThriftUtils.retrieveThriftClass;
import static com.google.cloud.pso.beam.common.formats.ThriftUtils.validateFieldAndReturn;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TTransportException;

/** A transport format handler for the thrift encoded data. */
public record ThriftTransportHandler(ThriftClass thriftClass)
    implements TransportFormats.Handler<TBase>, Serializable {

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

  public ThriftTransportHandler(String thriftClassName) {
    this(
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
              element.getFieldValue(thriftClass().retrieveFieldEnumValueByFieldName(propertyName)))
          .longValue();
      case TType.I32 -> ((Integer)
              element.getFieldValue(thriftClass().retrieveFieldEnumValueByFieldName(propertyName)))
          .longValue();
      case TType.I64 -> ((Long)
          element.getFieldValue(thriftClass().retrieveFieldEnumValueByFieldName(propertyName)));
      case TType.DOUBLE -> ((Double)
              element.getFieldValue(thriftClass().retrieveFieldEnumValueByFieldName(propertyName)))
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
              element.getFieldValue(thriftClass().retrieveFieldEnumValueByFieldName(propertyName)))
          .doubleValue();
      case TType.I32 -> ((Integer)
              element.getFieldValue(thriftClass().retrieveFieldEnumValueByFieldName(propertyName)))
          .doubleValue();
      case TType.I64 -> ((Long)
              element.getFieldValue(thriftClass().retrieveFieldEnumValueByFieldName(propertyName)))
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
