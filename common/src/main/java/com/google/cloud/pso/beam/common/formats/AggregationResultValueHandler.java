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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.Optional;

/** A transport format handler for the aggregation results transports. */
public record AggregationResultValueHandler()
    implements TransportFormats.Handler<AggregationResultValue>, Serializable {

  static final ThreadLocal<Kryo> KRYO =
      new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
          var kryo = new Kryo();
          kryo.register(AggregationResultValue.class);
          kryo.register(java.util.HashMap.class);
          return kryo;
        }
      };

  byte[] writeKryo(AggregationResultValue element) {
    try {
      var baos = new ByteArrayOutputStream();
      var output = new Output(baos);
      KRYO.get().writeObject(output, element);
      output.close();
      return baos.toByteArray();
    } catch (KryoException ex) {
      throw new IllegalArgumentException(
          "The serialization of the provided element was not possible.", ex);
    }
  }

  AggregationResultValue readKryo(byte[] bytes) {
    try {
      var bais = new ByteArrayInputStream(bytes);
      return KRYO.get().readObject(new Input(bais), AggregationResultValue.class);
    } catch (Exception ex) {
      throw new IllegalArgumentException(
          "The provided byte array did not conform the expected structure.", ex);
    }
  }

  @Override
  public byte[] encode(AggregationResultValue element) {
    return writeKryo(element);
  }

  @Override
  public AggregationResultValue decode(byte[] encodedElement) {
    return readKryo(encodedElement);
  }

  void validateKey(String key) {
    Preconditions.checkNotNull(
        key,
        "The provided element ("
            + key
            + ") does not have the expected structure. "
            + "This value handler assumes the encoded value as an aggregation "
            + "result structure.");
  }

  @Override
  public Long longValue(AggregationResultValue element, String propertyName) {
    validateKey(element.key());
    return element.mappedResults().get(propertyName).longValue();
  }

  @Override
  public Double doubleValue(AggregationResultValue element, String propertyName) {
    validateKey(element.key());
    return element.mappedResults().get(propertyName).doubleValue();
  }

  /**
   * Assumes is used to retrieve the aggregation's key. It discards previously computed aggregation
   * key components (since this is coming from a previous aggregation they keys will take the form
   * [field#field-value#result-info].
   *
   * @param element the decoded map for the aggregation result.
   * @param propertyName the key property name
   * @return the aggregation key field-value component.
   */
  @Override
  public String stringValue(AggregationResultValue element, String propertyName) {
    // outer map should only contain a non-null value as the key
    var storedKey = Optional.ofNullable(element.key());
    return storedKey
        // the property name provided should appear as a component part of the key
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
