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
package com.google.cloud.pso.beam.transforms.aggregations;

import com.google.cloud.pso.beam.common.formats.TransportFormats;
import com.google.cloud.pso.beam.common.transport.EventTransport;
import com.google.cloud.pso.beam.options.CountByFieldsAggregationOptions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a count aggregation which groups data based on a set of configured fields. Also
 * enables the configuration of the window's length and trigger frequencies.
 */
public class CountByFieldsAggregation
    extends PTransform<
        PCollection<? extends EventTransport>, PCollection<AggregationResultTransport>> {

  private static final Logger LOG = LoggerFactory.getLogger(CountByFieldsAggregation.class);

  CountByFieldsAggregation() {}

  public static CountByFieldsAggregation create() {
    return new CountByFieldsAggregation();
  }

  Trigger getTrigger(Integer elementCount, Integer triggerSeconds) {
    return AfterWatermark.pastEndOfWindow()
        .withEarlyFirings(
            AfterFirst.of(
                AfterPane.elementCountAtLeast(elementCount),
                AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardSeconds(triggerSeconds))));
  }

  Window<KV<String, Long>> getWindow(CountByFieldsAggregationOptions options) {
    var window =
        Window.<KV<String, Long>>into(
                FixedWindows.of(Duration.standardMinutes(options.getAggregationWindowInMinutes())))
            .triggering(
                getTrigger(
                    options.getAggregationPartialTriggerEventCount(),
                    options.getAggregationPartialTriggerSeconds()))
            .withAllowedLateness(
                Duration.standardMinutes(options.getAggregationAllowedLatenessInMinutes()));
    if (options.getAggregationDiscardPartialResults()) {
      window = window.discardingFiredPanes();
    } else {
      window = window.accumulatingFiredPanes();
    }

    return window;
  }

  @Override
  public PCollection<AggregationResultTransport> expand(
      PCollection<? extends EventTransport> input) {
    var options = input.getPipeline().getOptions().as(CountByFieldsAggregationOptions.class);
    input
        .getPipeline()
        .getCoderRegistry()
        .registerCoderForClass(CountTransport.class, CountTransportCoder.of());
    input
        .getPipeline()
        .getCoderRegistry()
        .registerCoderForClass(AggregationResultTransport.class, CountResultTransportCoder.of());

    return input
        .apply("ToAggregationTransport", ParDo.of(new ToCountableTransports()))
        .apply(
            "MapToKV",
            MapElements.into(
                    TypeDescriptors.lists(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs())))
                .via(
                    tr ->
                        tr.getMappedValues().entrySet().stream()
                            .map(
                                entry ->
                                    KV.of(
                                        tr.getAggregationKey() + "#" + entry.getKey(),
                                        entry.getValue()))
                            .toList()))
        .apply("Flat", Flatten.iterables())
        .apply("Window", getWindow(options))
        .apply("CountEventsPerKey", Count.perKey())
        .apply("ToAggregationResults", ParDo.of(new ToCountResults()));
  }

  static class ToCountableTransports extends DoFn<EventTransport, CountTransport> {

    private CountByFieldsAggregationConfiguration config;

    @StartBundle
    public void startBundle(PipelineOptions options) {
      config = options.as(CountByFieldsAggregationOptions.class).getCountConfiguration();
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(CountTransport.fromTransportAndConfiguration(context.element(), config));
    }
  }

  static class ToCountResults extends DoFn<KV<String, Long>, AggregationResultTransport> {

    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window, PaneInfo pane) {
      context.output(
          CountResultTransport.fromKV(
              context.element(), Instant.now(), window.maxTimestamp(), pane));
    }
  }

  static class CountTransportCoder extends CustomCoder<CountTransport> {

    // Aggregation key cannot be null
    private static final Coder<String> KEY_CODER = StringUtf8Coder.of();
    // A message's attributes can be null.
    private static final Coder<Map<String, String>> HEADERS_CODER =
        NullableCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    // A message's messageId may be null at some moments in the execution
    private static final Coder<String> ID_CODER = NullableCoder.of(StringUtf8Coder.of());

    public static Coder<CountTransport> of(
        TypeDescriptor<AggregationTransport<String, Long>> ignored) {
      return of();
    }

    public static CountTransportCoder of() {
      return new CountTransportCoder();
    }

    @Override
    public void encode(CountTransport value, OutputStream outStream) throws IOException {
      KEY_CODER.encode(value.getAggregationKey(), outStream);
      HEADERS_CODER.encode(value.getHeaders(), outStream);
      ID_CODER.encode(value.getId(), outStream);
    }

    @Override
    public CountTransport decode(InputStream inStream) throws IOException {
      var key = KEY_CODER.decode(inStream);
      var headers = HEADERS_CODER.decode(inStream);
      var id = ID_CODER.decode(inStream);
      return new CountTransport(id, headers, key);
    }
  }

  static class CountResultTransportCoder extends CustomCoder<CountResultTransport> {

    // Aggregation key cannot be null
    private static final Coder<String> KEY_CODER = StringUtf8Coder.of();
    // A message's attributes can be null.
    private static final Coder<Map<String, String>> HEADERS_CODER =
        NullableCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    // The result of the aggregation is assumed a Long for now
    private static final Coder<Long> RESULT_CODER = NullableCoder.of(VarLongCoder.of());

    public static Coder<CountResultTransport> of(
        TypeDescriptor<AggregationTransport<String, Long>> ignored) {
      return of();
    }

    public static CountResultTransportCoder of() {
      return new CountResultTransportCoder();
    }

    @Override
    public void encode(CountResultTransport value, OutputStream outStream) throws IOException {
      KEY_CODER.encode(value.getAggregationKey(), outStream);
      HEADERS_CODER.encode(value.getHeaders(), outStream);
      RESULT_CODER.encode(value.getResult(), outStream);
    }

    @Override
    public CountResultTransport decode(InputStream inStream) throws IOException {
      var key = KEY_CODER.decode(inStream);
      var headers = HEADERS_CODER.decode(inStream);
      var result = RESULT_CODER.decode(inStream);
      return new CountResultTransport(key, result, headers);
    }
  }

  @DefaultCoder(CountTransportCoder.class)
  static class CountTransport implements AggregationTransport<String, Long> {

    final String id;
    final Map<String, String> headers;
    final String aggregationKey;

    CountTransport(String id, Map<String, String> headers, String aggregationKey) {
      this.id = id;
      this.headers = headers;
      this.aggregationKey = aggregationKey;
    }

    @Override
    public String getAggregationKey() {
      return aggregationKey;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public Map<String, String> getHeaders() {
      return headers;
    }

    @Override
    public byte[] getData() {
      return new byte[0];
    }

    @Override
    public Map<String, Long> getMappedValues() {
      return Map.of("count", 1L);
    }

    public static CountTransport fromTransportAndConfiguration(
        EventTransport transport, CountByFieldsAggregationConfiguration configuration) {
      var format = configuration.getFormat();
      var formatHandlerFactory = TransportFormats.handlerFactory(format);

      var handler =
          switch (format) {
            case THRIFT -> formatHandlerFactory.apply(configuration.getClassName());
            case AVRO -> formatHandlerFactory.apply(configuration.getAvroSchema());
          };

      var decodedData = handler.decode(transport.getData());
      var aggregationKey =
          configuration.getKeyFields().stream()
              .map(keyField -> keyField + "#" + handler.stringValue(decodedData, keyField))
              .collect(Collectors.joining("#"));
      return new CountTransport(transport.getId(), transport.getHeaders(), aggregationKey);
    }
  }

  @DefaultCoder(CountResultTransportCoder.class)
  static class CountResultTransport implements AggregationResultTransport<String, Long> {

    private final String aggregationKey;
    private final Long result;
    private final Map<String, String> headers;

    public CountResultTransport(String aggregationKey, Long result, Map<String, String> headers) {
      this.aggregationKey = aggregationKey;
      this.result = result;
      this.headers = headers;
    }

    @Override
    public String getAggregationKey() {
      return aggregationKey;
    }

    @Override
    public Long getResult() {
      return result;
    }

    @Override
    public String getId() {
      return aggregationKey;
    }

    @Override
    public Map<String, String> getHeaders() {
      return headers;
    }

    @Override
    public byte[] getData() {
      return new byte[0];
    }

    @Override
    public ResultType getType() {
      return ResultType.LONG;
    }

    public static AggregationResultTransport<String, Long> fromKV(
        KV<String, Long> kv, Instant elementTimestamp, Instant windowEndTimestamp, PaneInfo pane) {
      var headers = Maps.<String, String>newHashMap();
      headers.put(AggregationResultTransport.EVENT_TIME_KEY, elementTimestamp.toString());
      headers.put(
          AggregationResultTransport.AGGREGATION_WINDOW_TIME_KEY, windowEndTimestamp.toString());
      headers.put(AggregationResultTransport.AGGREGATION_VALUE_TIMING_KEY, pane.getTiming().name());
      headers.put(
          AggregationResultTransport.AGGREGATION_VALUE_IS_FINAL_KEY, String.valueOf(pane.isLast()));

      return new CountResultTransport(kv.getKey(), kv.getValue(), headers)
          .withAggregationName("count");
    }
  }
}
