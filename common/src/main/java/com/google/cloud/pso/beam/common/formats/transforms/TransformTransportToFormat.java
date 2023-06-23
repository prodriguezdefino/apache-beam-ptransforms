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
package com.google.cloud.pso.beam.common.formats.transforms;

import static com.google.cloud.pso.beam.common.formats.AvroUtils.*;
import static com.google.cloud.pso.beam.common.formats.ThriftUtils.*;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.pso.beam.common.formats.InputFormatConfiguration;
import com.google.cloud.pso.beam.common.formats.JsonUtils;
import com.google.cloud.pso.beam.common.formats.options.TransportFormatOptions;
import com.google.cloud.pso.beam.common.transport.CommonErrorTransport;
import com.google.cloud.pso.beam.common.transport.ErrorTransport;
import com.google.cloud.pso.beam.common.transport.EventTransport;
import java.io.ByteArrayInputStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.thrift.TBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transform in charge of obtaining a format <T> ready to be ingested into BigQuery.
 *
 * @param <T> A format compatible with BigQueryIO direct usage.
 */
public abstract class TransformTransportToFormat<T>
    extends PTransform<PCollection<EventTransport>, PCollectionTuple> {

  private static final Logger LOG = LoggerFactory.getLogger(TransformTransportToFormat.class);
  public static final TupleTag<ErrorTransport> FAILED_EVENTS = new TupleTag<>() {};

  TransformTransportToFormat() {}

  public static TransformToRows transformToRows() {
    return new TransformToRows();
  }

  public static TransformToGenericRecords transformToGenericRecords() {
    return new TransformToGenericRecords();
  }

  public static TransformToTableRows transformToTableRows() {
    return new TransformToTableRows();
  }

  public static TransformErrorTransportsToRow transformErrorsToRows() {
    return new TransformErrorTransportsToRow();
  }

  public static TupleTag<TableRow> successfulTableRows() {
    return TransformToTableRows.SUCCESSFULLY_PROCESSED_EVENTS;
  }

  public static TupleTag<Row> successfulRows() {
    return TransformToRows.SUCCESSFULLY_PROCESSED_EVENTS;
  }

  public static TupleTag<GenericRecord> successfulGenericRecords() {
    return TransformToGenericRecords.SUCCESSFULLY_PROCESSED_EVENTS;
  }

  abstract static class TransformTransport<T> extends DoFn<EventTransport, T> {

    protected Schema beamSchema;
    protected org.apache.avro.Schema avroSchema;
    protected org.everit.json.schema.Schema jsonSchema;
    protected Class<? extends TBase<?, ?>> thriftClass;
    protected final InputFormatConfiguration.FormatConfiguration config;

    public TransformTransport(InputFormatConfiguration.FormatConfiguration config) {
      this.config = config;
    }

    @Setup
    public void setup() throws Exception {
      switch (config.format()) {
        case THRIFT -> {
          var className = ((InputFormatConfiguration.ThriftFormat) config).className();
          thriftClass = retrieveThriftClass(className);
          avroSchema = retrieveAvroSchemaFromThriftClassName(className);
          break;
        }
        case AVRO -> {
          var schemaLocation = ((InputFormatConfiguration.AvroFormat) config).schemaLocation();
          avroSchema = retrieveAvroSchemaFromLocation(schemaLocation);
          break;
        }
        case JSON -> {
          var schemaLocation = ((InputFormatConfiguration.JSONFormat) config).schemaLocation();
          jsonSchema = JsonUtils.retrieveJsonSchemaFromLocation(schemaLocation);
          avroSchema = JsonUtils.jsonSchemaToAvroSchema(jsonSchema);
          beamSchema = AvroUtils.toBeamSchema(avroSchema);
        }
        default -> throw new IllegalArgumentException("Format not supported: " + config.format());
      }
      setupSpecific();
    }

    protected void setupSpecific() throws Exception {}

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
      try {
        processSpecific(context);
      } catch (Exception ex) {
        var msg = "Errors occurred while trying to transform the transport to the format.";
        LOG.error(msg, ex);
        context.output(FAILED_EVENTS, CommonErrorTransport.of(context.element(), msg, ex));
      }
    }

    protected abstract void processSpecific(ProcessContext context) throws Exception;
  }

  public static class TransformToRows extends TransformTransportToFormat<Row> {

    public static TupleTag<Row> SUCCESSFULLY_PROCESSED_EVENTS = new TupleTag<>() {};

    @Override
    public PCollectionTuple expand(PCollection<EventTransport> input) {
      var options = input.getPipeline().getOptions().as(TransportFormatOptions.class);
      return input.apply(
          "TransformToRow",
          ParDo.of(new TransformTransportToRow(InputFormatConfiguration.fromOptions(options)))
              .withOutputTags(SUCCESSFULLY_PROCESSED_EVENTS, TupleTagList.of(FAILED_EVENTS)));
    }

    static class TransformTransportToRow extends TransformTransport<Row> {

      public TransformTransportToRow(InputFormatConfiguration.FormatConfiguration config) {
        super(config);
      }

      @Override
      public void setupSpecific() throws Exception {
        beamSchema = AvroUtils.toBeamSchema(avroSchema);
      }

      @Override
      public void processSpecific(ProcessContext context) throws Exception {
        context.output(
            SUCCESSFULLY_PROCESSED_EVENTS,
            retrieveRowFromTransport(context.element(), config, beamSchema, avroSchema));
      }
    }
  }

  public static class TransformToGenericRecords extends TransformTransportToFormat<GenericRecord> {

    public static TupleTag<GenericRecord> SUCCESSFULLY_PROCESSED_EVENTS = new TupleTag<>() {};

    @Override
    public PCollectionTuple expand(PCollection<EventTransport> input) {
      var options = input.getPipeline().getOptions().as(TransportFormatOptions.class);
      return input.apply(
          "TransformToGenericRecord",
          ParDo.of(
                  new TransformTransportToGenericRecord(
                      InputFormatConfiguration.fromOptions(options)))
              .withOutputTags(SUCCESSFULLY_PROCESSED_EVENTS, TupleTagList.of(FAILED_EVENTS)));
    }

    static class TransformTransportToGenericRecord extends TransformTransport<GenericRecord> {

      public TransformTransportToGenericRecord(
          InputFormatConfiguration.FormatConfiguration config) {
        super(config);
      }

      @Override
      public void processSpecific(ProcessContext context) throws Exception {
        context.output(
            SUCCESSFULLY_PROCESSED_EVENTS,
            retrieveGenericRecordFromTransport(context.element(), config, beamSchema, avroSchema));
      }
    }
  }

  public static class TransformToTableRows extends TransformTransportToFormat<TableRow> {

    public static TupleTag<TableRow> SUCCESSFULLY_PROCESSED_EVENTS = new TupleTag<>() {};

    @Override
    public PCollectionTuple expand(PCollection<EventTransport> input) {
      var options = input.getPipeline().getOptions().as(TransportFormatOptions.class);
      return input.apply(
          "TransformToTableRow",
          ParDo.of(new TransformTransportToTableRow(InputFormatConfiguration.fromOptions(options)))
              .withOutputTags(SUCCESSFULLY_PROCESSED_EVENTS, TupleTagList.of(FAILED_EVENTS)));
    }

    static class TransformTransportToTableRow extends TransformTransport<TableRow> {

      public TransformTransportToTableRow(InputFormatConfiguration.FormatConfiguration config) {
        super(config);
      }

      @Override
      public void processSpecific(ProcessContext context) throws Exception {
        context.output(
            SUCCESSFULLY_PROCESSED_EVENTS,
            retrieveTableRowFromTransport(context.element(), config, avroSchema));
      }
    }
  }

  public static class TransformErrorTransportsToRow
      extends PTransform<PCollection<ErrorTransport>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollection<ErrorTransport> input) {
      return input
          .apply("TransformErrorsToRow", ParDo.of(new TransformErrorTransportToRow()))
          .setRowSchema(ErrorTransport.ERROR_ROW_SCHEMA);
    }

    static class TransformErrorTransportToRow extends DoFn<ErrorTransport, Row> {

      @ProcessElement
      public void processElement(ProcessContext context) throws Exception {
        context.output(context.element().toRow());
      }
    }
  }

  public static Row retrieveRowFromTransport(
      EventTransport transport,
      InputFormatConfiguration.FormatConfiguration config,
      Schema beamSchema,
      org.apache.avro.Schema avroSchema) {

    return switch (config.format()) {
      case JSON -> BigQueryUtils.toBeamRow(
          beamSchema, (TableRow) config.handler().decode(transport.getData()));
      default -> AvroUtils.toBeamRowStrict(
          retrieveGenericRecordFromTransport(transport, config, beamSchema, avroSchema),
          beamSchema);
    };
  }

  static GenericRecord retrieveGenericRecordFromTransport(
      EventTransport transport,
      InputFormatConfiguration.FormatConfiguration config,
      Schema beamSchema,
      org.apache.avro.Schema avroSchema) {
    try {
      return switch (config.format()) {
        case AVRO -> {
          yield (GenericRecord) config.handler().decode(transport.getData());
        }
        case THRIFT -> {
          var thriftObject = (TBase<?, ?>) config.handler().decode(transport.getData());
          yield retrieveGenericRecordFromThriftData(thriftObject, avroSchema);
        }
        case JSON -> {
          var tableRow = (TableRow) config.handler().decode(transport.getData());
          var beamRow = BigQueryUtils.toBeamRow(beamSchema, tableRow);
          yield AvroUtils.toGenericRecord(beamRow);
        }
        default -> throw new RuntimeException("Format not implemented " + config.format());
      };
    } catch (Exception ex) {
      var msg = "Error while trying to retrieve a generic record from the transport object.";
      LOG.error(msg, ex);
      throw new RuntimeException(msg, ex);
    }
  }

  static TableRow retrieveTableRowFromTransport(
      EventTransport transport,
      InputFormatConfiguration.FormatConfiguration config,
      org.apache.avro.Schema avroSchema) {
    try {
      return switch (config.format()) {
        case AVRO -> {
          var genericRecord = (GenericRecord) config.handler().decode(transport.getData());
          yield BigQueryUtils.convertGenericRecordToTableRow(
              genericRecord, BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(avroSchema)));
        }
        case THRIFT -> {
          var thriftObject = (TBase<?, ?>) config.handler().decode(transport.getData());
          var genericRecord = retrieveGenericRecordFromThriftData(thriftObject, avroSchema);
          yield BigQueryUtils.convertGenericRecordToTableRow(
              genericRecord, BigQueryUtils.toTableSchema(AvroUtils.toBeamSchema(avroSchema)));
        }
        case JSON -> {
          var bais = new ByteArrayInputStream(transport.getData());
          @SuppressWarnings("deprecation")
          var tableRow = TableRowJsonCoder.of().decode(bais, Coder.Context.OUTER);
          yield tableRow;
        }
        default -> throw new RuntimeException("Format not implemented " + config.format());
      };
    } catch (Exception ex) {
      var msg = "Error while trying to retrieve a generic record from the transport object.";
      LOG.error(msg, ex);
      throw new RuntimeException(msg, ex);
    }
  }

  public static Schema retrieveRowSchema(TransportFormatOptions options) {
    return switch (options.getTransportFormat()) {
      case THRIFT -> retrieveRowSchema(options.getThriftClassName());
      case AVRO -> AvroUtils.toBeamSchema(
          retrieveAvroSchemaFromLocation(options.getSchemaFileLocation()));
      case JSON -> AvroUtils.toBeamSchema(
          JsonUtils.jsonSchemaToAvroSchema(
              JsonUtils.retrieveJsonSchemaFromLocation(options.getSchemaFileLocation())));
      default -> throw new IllegalArgumentException(
          "Event format has not being implemented for ingestion: " + options.getTransportFormat());
    };
  }

  public static org.apache.avro.Schema retrieveAvroSchema(TransportFormatOptions options) {
    return switch (options.getTransportFormat()) {
      case THRIFT -> retrieveAvroSchemaFromThriftClassName(options.getThriftClassName());
      case AVRO -> retrieveAvroSchemaFromLocation(options.getSchemaFileLocation());
      case JSON -> JsonUtils.jsonSchemaToAvroSchema(
          JsonUtils.retrieveJsonSchemaFromLocation(options.getSchemaFileLocation()));
      default -> throw new IllegalArgumentException(
          "Event format has not being implemented for ingestion: " + options.getTransportFormat());
    };
  }

  public static Schema retrieveRowSchema(String thriftClassName) {
    return AvroUtils.toBeamSchema(retrieveAvroSchemaFromThriftClassName(thriftClassName));
  }
}
