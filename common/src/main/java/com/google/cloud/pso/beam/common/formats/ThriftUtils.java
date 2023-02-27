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

import java.util.Map;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;

/** */
public class ThriftUtils {

  public static Class<? extends TBase<?, ?>> retrieveThriftClass(String className) {
    try {
      return (Class<? extends TBase<?, ?>>)
          Class.forName(className, true, Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException("Thrift class " + className + " was not found", ex);
    }
  }

  public static Class<? extends TFieldIdEnum> retrieveThirftFieldEnum(String className) {
    try {
      return (Class<? extends TFieldIdEnum>)
          Class.forName(
              className + "$_Fields", true, Thread.currentThread().getContextClassLoader());
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException("Thrift class " + className + " was not found", ex);
    }
  }

  public static TBase<?, ?> getThriftObjectFromData(TBase<?, ?> emptyInstance, byte[] data) {
    try {
      var deserializer = new TDeserializer(new TBinaryProtocol.Factory());
      deserializer.deserialize(emptyInstance, data);
      return emptyInstance;
    } catch (TTransportException e) {
      throw new RuntimeException("Error while creating a TDeserializer.", e);
    } catch (TException ex) {
      throw new RuntimeException("Can't serialize instance.", ex);
    }
  }

  public static FieldMetaData validateFieldAndReturn(TBase element, String propertyName) {
    var clazz = (Class<? extends TBase>) element.getClass();
    return ((Map<? extends TBase, FieldMetaData>) FieldMetaData.getStructMetaDataMap(clazz))
        .values().stream()
            .filter(f -> f.fieldName.equals(propertyName))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Provided property name does not exists in the type "
                            + "as a simple value (structs, composite or "
                            + "collection types are not supported)."));
  }
}
