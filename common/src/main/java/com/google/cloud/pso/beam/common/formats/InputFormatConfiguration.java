/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
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
