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

import com.google.cloud.pso.beam.common.formats.options.TransportFormatOptions;
import java.io.Serializable;
import org.apache.avro.generic.GenericRecord;
import org.apache.thrift.TBase;
import org.json.JSONObject;

/** */
public class InputFormatConfiguration {
  public sealed interface FormatConfiguration<T>
      permits ThriftFormat, AvroFormat, AggregationResultFormat, JSONFormat {

    TransportFormats.Format format();

    TransportFormats.Handler<T> handler();
  }

  public static <T> FormatConfiguration<? extends Object> fromOptions(
      TransportFormatOptions options) {
    return switch (options.getTransportFormat()) {
      case THRIFT -> new ThriftFormat(options.getThriftClassName());
      case AVRO -> new AvroFormat(options.getSchemaFileLocation());
      case JSON -> new JSONFormat(options.getSchemaFileLocation());
      default -> throw new IllegalArgumentException(
          "Format not supported with options info: " + options.getTransportFormat());
    };
  }

  public record ThriftFormat(String className) implements FormatConfiguration<TBase>, Serializable {

    @Override
    public TransportFormats.Format format() {
      return TransportFormats.Format.THRIFT;
    }

    @Override
    public TransportFormats.Handler<TBase> handler() {
      return TransportFormats.<TBase>handlerFactory(format()).apply(className);
    }
  }

  public record JSONFormat(String schemaLocation)
      implements FormatConfiguration<JSONObject>, Serializable {

    @Override
    public TransportFormats.Format format() {
      return TransportFormats.Format.JSON;
    }

    @Override
    public TransportFormats.Handler<JSONObject> handler() {
      return TransportFormats.<JSONObject>handlerFactory(format()).apply(schemaLocation);
    }
  }

  public record AvroFormat(String schemaLocation)
      implements FormatConfiguration<GenericRecord>, Serializable {

    @Override
    public TransportFormats.Format format() {
      return TransportFormats.Format.AVRO;
    }

    @Override
    public TransportFormats.Handler<GenericRecord> handler() {
      return TransportFormats.<GenericRecord>handlerFactory(format()).apply(schemaLocation);
    }
  }

  public record AggregationResultFormat()
      implements FormatConfiguration<AggregationResultValue>, Serializable {

    @Override
    public TransportFormats.Format format() {
      return TransportFormats.Format.AGGREGATION_RESULT;
    }

    @Override
    public TransportFormats.Handler<AggregationResultValue> handler() {
      return TransportFormats.<AggregationResultValue>handlerFactory(format()).apply(null);
    }
  }
}
