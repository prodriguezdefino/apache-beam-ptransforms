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

import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Defines the supported formats, encoding and decoding functionalities and data extraction methods.
 */
public class TransportFormats {

  static final Map<String, Handler> HANDLERS = Maps.newConcurrentMap();

  public enum Format {
    THRIFT,
    AVRO,
    AGGREGATION_RESULT,
    JSON
  }

  @SuppressWarnings("unchecked")
  public static <T> Function<String, Handler<T>> handlerFactory(Format format) {
    return switch (format) {
      case THRIFT -> className ->
          HANDLERS.computeIfAbsent(className, key -> new ThriftTransportHandler(key));
      case AVRO -> avroSchemaLocation ->
          HANDLERS.computeIfAbsent(
              avroSchemaLocation, key -> new AvroGenericRecordTransportHandler(key));
      case AGGREGATION_RESULT -> dummy ->
          HANDLERS.computeIfAbsent(
              Format.AGGREGATION_RESULT.name(), key -> new AggregationResultValueHandler());
      case JSON -> jsonSchemaLocation ->
          HANDLERS.computeIfAbsent(jsonSchemaLocation, key -> new JsonTransportHandler(key));
    };
  }

  public sealed interface Handler<T>
      permits ThriftTransportHandler,
          AvroGenericRecordTransportHandler,
          AggregationResultValueHandler,
          JsonTransportHandler {

    byte[] encode(T element);

    T decode(byte[] encodedElement);

    Long longValue(T element, String propertyName);

    Double doubleValue(T element, String propertyName);

    String stringValue(T element, String propertyName);

    static <In, Res> Res extractDataWalker(
        In input,
        String propertyName,
        BiFunction<In, String, In> navigator,
        BiFunction<In, String, Res> extractor) {
      var propertyNameParts = propertyName.split("\\.");
      var propName = propertyNameParts[0];
      if (propertyNameParts.length > 1) {
        return extractDataWalker(
            Optional.ofNullable(navigator.apply(input, propName))
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            String.format(
                                "Property name %s did not contained a navigable object in the provided object (%s).",
                                propName, input.toString()))),
            Arrays.stream(propertyNameParts).skip(1).collect(Collectors.joining(".")),
            navigator,
            extractor);
      }
      return extractor.apply(input, propName);
    }
  }
}
