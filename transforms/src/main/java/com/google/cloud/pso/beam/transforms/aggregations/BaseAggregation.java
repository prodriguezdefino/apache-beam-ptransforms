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

import com.google.cloud.pso.beam.common.Functions;
import com.google.cloud.pso.beam.common.formats.AggregationResultValue;
import com.google.cloud.pso.beam.common.formats.InputFormatConfiguration.*;
import com.google.cloud.pso.beam.common.formats.InputFormatConfiguration.FormatConfiguration;
import com.google.cloud.pso.beam.common.formats.TransportFormats;
import com.google.cloud.pso.beam.common.transport.AggregationResultTransport;
import com.google.cloud.pso.beam.common.transport.AggregationTransport;
import com.google.cloud.pso.beam.common.transport.EventTransport;
import com.google.cloud.pso.beam.common.transport.Transport;
import com.google.cloud.pso.beam.transforms.aggregations.Configuration.*;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the common template structure for aggregations. It defines abstract methods that
 * enable specific aggregations to implement how to encode key, values and results, also to define
 * how to extract data from the transport objects (for key and values) and the specific aggregation
 * PTransform (which can be any of those existing in Beam or a custom one given the existing type
 * signatures).
 *
 * @param <Key> The aggregation's key type
 * @param <Value> The type of the value to be aggregated
 * @param <Res> the aggregation's result type
 */
public abstract class BaseAggregation<Key, Value, Res>
    extends PTransform<
        PCollection<? extends Transport>, PCollection<AggregationResultTransport<Key, Res>>> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseAggregation.class);

  /**
   * A function that can create the specific handler to use on a transport object given the
   * transport format configuration.
   */
  protected static final SerializableFunction<FormatConfiguration, TransportFormats.Handler>
      FORMAT_HANDLER_FUNC =
          configuration -> {
            return switch (configuration.format()) {
              case AVRO -> TransportFormats.handlerFactory(TransportFormats.Format.AVRO)
                  .apply(((AvroFormat) configuration).schemaLocation());
              case THRIFT -> TransportFormats.handlerFactory(TransportFormats.Format.THRIFT)
                  .apply(((ThriftFormat) configuration).className());
              case AGGREGATION_RESULT -> TransportFormats.handlerFactory(
                      TransportFormats.Format.AGGREGATION_RESULT)
                  .apply(null);
              case JSON -> TransportFormats.handlerFactory(TransportFormats.Format.JSON)
                  .apply(((JSONFormat) configuration).schemaLocation());
            };
          };

  /** A function that decodes a raw bytes array given the expected transport configuration. */
  static final SerializableBiFunction<FormatConfiguration, byte[], Object> EVENT_DECODER =
      (configuration, data) -> {
        return FORMAT_HANDLER_FUNC.apply(configuration).decode(data);
      };

  protected final AggregationConfiguration configuration;

  protected BaseAggregation(AggregationConfiguration configuration, String name) {
    super(name);
    this.configuration = configuration;
  }

  /**
   * The coder needed for the aggregation key given the expected type.
   *
   * @return A coder for the aggregation's key
   */
  protected abstract Coder<Key> keyCoder();

  /**
   * The coder needed for the aggregation result given the expected type.
   *
   * @return A coder for the aggregation's result
   */
  protected abstract Coder<Res> resultCoder();

  /**
   * Creates a function that given a list of field names and the decoded transport object, returns
   * the key expected for the aggregation.
   *
   * @return The key extraction function.
   */
  protected abstract SerializableBiFunction<List<String>, Object, Key> keyExtractorFunction();

  /**
   * Creates a function that given a list of field names and the decoded transport object, returns a
   * map of the desired values mapped by their name.
   *
   * @return The mapped values extraction function.
   */
  protected abstract SerializableBiFunction<List<String>, Object, Map<String, Value>>
      valuesExtractorFunction();

  /**
   * A map elements function that will transform the incoming aggregation transports into the list
   * of elements to be aggregated. Given a single aggregation transport object, we expect only 1 key
   * to be extracted, but we can capture multiple different fields to be aggregated.
   *
   * @return The map elements PTransform that prepares the aggregation transport for later
   *     aggregation by key.
   */
  protected abstract MapElements<BaseAggregationTransport<Key, Value>, List<KV<Key, Res>>>
      transportMapper();

  /**
   * Creates the PTransform that implements the desired aggregation.
   *
   * @return the aggregation to perform
   */
  protected abstract PTransform<PCollection<KV<Key, Res>>, PCollection<KV<Key, Res>>> aggregation();

  @Override
  public PCollection<AggregationResultTransport<Key, Res>> expand(
      PCollection<? extends Transport> input) {
    var aggTransportCoder =
        BaseAggregationTransportCoder.of(
            Functions.curry(EVENT_DECODER).apply(configuration.format()),
            Functions.curry(keyExtractorFunction()).apply(configuration.keyFields()),
            Functions.curry(valuesExtractorFunction()).apply(configuration.valueFields()));
    var aggResultTransportCoder = AggregationResultTransportCoder.of(keyCoder(), resultCoder());
    var aggregationName = configuration.name();

    return input
        .apply(
            "ToAggregationTransport",
            ParDo.of(
                new ToAggregationTransports<>(
                    keyExtractorFunction(), valuesExtractorFunction(), configuration)))
        .setCoder(aggTransportCoder)
        .apply("MapToKV", transportMapper())
        .apply("Flat", Flatten.iterables())
        .apply("Window", createWindow(configuration.window()))
        .apply(aggregationName, aggregation())
        .apply("ToAggregationResults", ParDo.of(new ToAggregationResults<>(aggregationName)))
        .setCoder(aggResultTransportCoder);
  }

  /**
   * Defines the window's trigger that will help to output data during the processing. Specific
   * aggregation implementations can override this behavior when needed.
   *
   * @param earlyFiring when false it will use the default trigger, if not early firings will be
   *     added.
   * @param elementCount when using early firings, the amount of elements that will be included
   *     before outputting
   * @param time when using early firings, the max amount of time that will be waiting before
   *     outputting data, if the element count was not already triggered before
   * @return the trigger behavior for the aggregation.
   */
  protected Trigger createTrigger(Boolean earlyFiring, Integer elementCount, Duration time) {
    Trigger trigger = null;
    if (earlyFiring) {
      trigger =
          AfterWatermark.pastEndOfWindow()
              .withEarlyFirings(
                  AfterFirst.of(
                      AfterPane.elementCountAtLeast(elementCount),
                      AfterProcessingTime.pastFirstElementInPane().plusDelayOf(time)));
    } else {
      trigger = AfterWatermark.pastEndOfWindow();
    }
    return Repeatedly.forever(trigger);
  }

  /**
   * Given the provided window configuration, it creates the window used by this aggregation.
   *
   * @param configuration the window configuration for this aggregation
   * @return the window behavior for the aggregation
   */
  protected Window<KV<Key, Res>> createWindow(WindowConfiguration configuration) {
    var window =
        Window.<KV<Key, Res>>into(FixedWindows.of(configuration.length()))
            .triggering(
                createTrigger(
                    configuration.withEarlyFirings(),
                    configuration.earlyFireCount(),
                    configuration.earlyFiringTime()));

    if (configuration.lateness().isLongerThan(Duration.ZERO))
      window =
          window.withAllowedLateness(configuration.lateness(), Window.ClosingBehavior.FIRE_ALWAYS);
    if (!configuration.shouldAccumulatePanes()) {
      window = window.discardingFiredPanes();
    } else {
      window = window.accumulatingFiredPanes();
    }
    return window;
  }

  static class ToAggregationTransports<Key, Value>
      extends DoFn<Transport, BaseAggregationTransport<Key, Value>> {

    private final SerializableBiFunction<List<String>, Object, Key> keyExtractor;
    private final SerializableBiFunction<List<String>, Object, Map<String, Value>> valuesExtractor;
    private final AggregationConfiguration config;

    public ToAggregationTransports(
        SerializableBiFunction<List<String>, Object, Key> keyExtractor,
        SerializableBiFunction<List<String>, Object, Map<String, Value>> valuesExtractor,
        AggregationConfiguration config) {
      this.keyExtractor = keyExtractor;
      this.valuesExtractor = valuesExtractor;
      this.config = config;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(
          BaseAggregationTransport.fromTransportAndConfigurationAndExtractors(
              context.element(), config, keyExtractor, valuesExtractor));
    }
  }

  static class ToAggregationResults<Key, Res>
      extends DoFn<KV<Key, Res>, AggregationResultTransport<Key, Res>> {

    private final String aggregationName;

    public ToAggregationResults(String aggregationName) {
      this.aggregationName = aggregationName;
    }

    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window, PaneInfo pane) {
      context.output(
          BaseAggregationResultTransport.fromKV(
              context.element(), Instant.now(), window.maxTimestamp(), pane, aggregationName));
    }
  }

  static class BaseAggregationTransportCoder<Key, Value>
      extends CustomCoder<BaseAggregationTransport<Key, Value>> {
    // An aggregation transport's payload may be null
    private static final Coder<byte[]> DATA_CODER = NullableCoder.of(ByteArrayCoder.of());
    // A message's attributes can be null.
    private static final Coder<Map<String, String>> HEADERS_CODER =
        NullableCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    // A message's messageId may be null at some moments in the execution
    private static final Coder<String> ID_CODER = NullableCoder.of(StringUtf8Coder.of());

    private final SerializableFunction<byte[], Object> dataDecoderFunction;
    private final SerializableFunction<Object, Key> keyExtractorFunction;
    private final SerializableFunction<Object, Map<String, Value>> valuesExtractorFunction;

    public BaseAggregationTransportCoder(
        SerializableFunction<byte[], Object> dataDecoderFunction,
        SerializableFunction<Object, Key> keyExtractorFunction,
        SerializableFunction<Object, Map<String, Value>> valuesExtractorFunction) {
      this.dataDecoderFunction = dataDecoderFunction;
      this.keyExtractorFunction = keyExtractorFunction;
      this.valuesExtractorFunction = valuesExtractorFunction;
    }

    public static <Key, Value> BaseAggregationTransportCoder<Key, Value> of(
        SerializableFunction<byte[], Object> dataDecoder,
        SerializableFunction<Object, Key> keyExtractor,
        SerializableFunction<Object, Map<String, Value>> valuesExtractor) {
      return new BaseAggregationTransportCoder<>(dataDecoder, keyExtractor, valuesExtractor);
    }

    @Override
    public void encode(BaseAggregationTransport<Key, Value> value, OutputStream outStream)
        throws IOException {
      DATA_CODER.encode(value.getData(), outStream);
      HEADERS_CODER.encode(value.getHeaders(), outStream);
      ID_CODER.encode(value.getId(), outStream);
    }

    @Override
    public BaseAggregationTransport<Key, Value> decode(InputStream inStream) throws IOException {
      var data = DATA_CODER.decode(inStream);
      var headers = HEADERS_CODER.decode(inStream);
      var id = ID_CODER.decode(inStream);
      return new BaseAggregationTransport<>(
          id, headers, data, dataDecoderFunction, keyExtractorFunction, valuesExtractorFunction);
    }
  }

  static class AggregationResultTransportCoder<Key, Res>
      extends CustomCoder<AggregationResultTransport<Key, Res>> {

    // A message's attributes can be null.
    private static final Coder<Map<String, String>> HEADERS_CODER =
        NullableCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    private final Coder<Key> keyCoder;
    private final Coder<Res> resultCoder;

    public AggregationResultTransportCoder(Coder<Key> keyCoder, Coder<Res> resultCoder) {
      this.keyCoder = keyCoder;
      this.resultCoder = resultCoder;
    }

    public static <Key, Res> AggregationResultTransportCoder<Key, Res> of(
        Coder<Key> keyCoder, Coder<Res> resultCoder) {
      return new AggregationResultTransportCoder<>(keyCoder, resultCoder);
    }

    @Override
    public void encode(AggregationResultTransport<Key, Res> value, OutputStream outStream)
        throws IOException {
      keyCoder.encode(value.getAggregationKey(), outStream);
      HEADERS_CODER.encode(value.getHeaders(), outStream);
      resultCoder.encode(value.getResult(), outStream);
    }

    @Override
    public AggregationResultTransport<Key, Res> decode(InputStream inStream) throws IOException {
      var key = keyCoder.decode(inStream);
      var headers = HEADERS_CODER.decode(inStream);
      var result = resultCoder.decode(inStream);
      return new BaseAggregationResultTransport<>(key, result, headers);
    }
  }

  /**
   * A base aggregation transport object, should be sufficient to capture the data send from
   * upstream to be aggregated.
   *
   * @param <Key> The type of the aggregation's key
   * @param <Value> the type of the values to be aggregated
   */
  protected static class BaseAggregationTransport<Key, Value>
      implements AggregationTransport<Key, Value> {
    private final String id;
    private final Map<String, String> headers;
    private final byte[] data;
    private final SerializableFunction<byte[], Object> dataDecoder;
    private final SerializableFunction<Object, Key> keyExtractor;
    private final SerializableFunction<Object, Map<String, Value>> valuesExtractor;

    private Key aggregationKey;
    private Map<String, Value> mappedValues;
    private Object decodedData;

    public BaseAggregationTransport(
        String id,
        Map<String, String> headers,
        byte[] data,
        SerializableFunction<byte[], Object> dataDecoder,
        SerializableFunction<Object, Key> keyExtractor,
        SerializableFunction<Object, Map<String, Value>> valuesExtractor) {
      this.id = id;
      this.headers = headers;
      this.data = data;
      this.dataDecoder = dataDecoder;
      this.keyExtractor = keyExtractor;
      this.valuesExtractor = valuesExtractor;
    }

    public Object decodedData() {
      if (decodedData == null) {
        decodedData = dataDecoder.apply(data);
      }
      return decodedData;
    }

    public byte[] getData() {
      return data;
    }

    @Override
    public Key getAggregationKey() {
      if (aggregationKey == null) {
        aggregationKey = keyExtractor.apply(decodedData());
      }
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
    public Map<String, Value> getMappedValues() {
      if (mappedValues == null) {
        mappedValues = valuesExtractor.apply(decodedData());
      }
      return mappedValues;
    }

    public BaseAggregationTransport<Key, Value> withDecodedData(Object decoded) {
      this.decodedData = decoded;
      return this;
    }

    public static <Key, Value>
        BaseAggregationTransport<Key, Value> fromTransportAndConfigurationAndExtractors(
            Transport transport,
            AggregationConfiguration configuration,
            SerializableBiFunction<List<String>, Object, Key> keyByFieldsExtractor,
            SerializableBiFunction<List<String>, Object, Map<String, Value>>
                valuesByFieldsExtractor) {

      if (transport instanceof EventTransport ev) {
        var curriedDecoder = Functions.curry(EVENT_DECODER).apply(configuration.format());
        var curriedKeyExtractor =
            Functions.curry(keyByFieldsExtractor).apply(configuration.keyFields());
        var curriedValuesExtractor =
            Functions.curry(valuesByFieldsExtractor).apply(configuration.valueFields());

        return new BaseAggregationTransport<>(
            ev.getId(),
            ev.getHeaders(),
            ev.getData(),
            curriedDecoder,
            curriedKeyExtractor,
            curriedValuesExtractor);
      } else if (transport instanceof AggregationResultTransport) {
        // we are processing a chained aggregation, the transport is the result of a previous
        // aggregation
        @SuppressWarnings("unchecked")
        var agg = (AggregationResultTransport<String, ? extends Number>) transport;
        // create the constant aggregation key and mapped values
        var mappedValues = new HashMap<String, Number>();
        mappedValues.put("result", agg.getResult());
        var aggregationKey = agg.getAggregationKey();
        var decodedData = new AggregationResultValue(aggregationKey, mappedValues);
        var dataDecoder =
            TransportFormats.<AggregationResultValue>handlerFactory(
                    TransportFormats.Format.AGGREGATION_RESULT)
                .apply(null);
        var encodedData = dataDecoder.encode(decodedData);
        var curriedKeyExtractor =
            Functions.curry(keyByFieldsExtractor).apply(configuration.keyFields());
        var curriedValuesExtractor =
            Functions.curry(valuesByFieldsExtractor).apply(configuration.valueFields());

        // create the aggregation transport with pre populated data
        return new BaseAggregationTransport<>(
                agg.getId(),
                agg.getHeaders(),
                encodedData,
                byteArray -> dataDecoder.decode(byteArray),
                curriedKeyExtractor,
                curriedValuesExtractor)
            .withDecodedData(decodedData);
      } else
        throw new IllegalArgumentException(
            "Currently aggregations can only be computed against event and aggregation result transports");
    }
  }

  /** A simple record containing the aggregations result. */
  protected record BaseAggregationResultTransport<Key, Res>(
      Key aggregationKey, Res result, Map<String, String> headers)
      implements AggregationResultTransport<Key, Res> {

    @Override
    public Key getAggregationKey() {
      return aggregationKey;
    }

    @Override
    public Res getResult() {
      return result;
    }

    @Override
    public String getId() {
      return UUID.randomUUID().toString();
    }

    @Override
    public Map<String, String> getHeaders() {
      return headers;
    }

    @Override
    public ResultType getType() {
      return ResultType.LONG;
    }

    public static <Key, Res> AggregationResultTransport<Key, Res> fromKV(
        KV<Key, Res> kv,
        Instant elementTimestamp,
        Instant windowEndTimestamp,
        PaneInfo pane,
        String aggregationName) {
      var headers = Maps.<String, String>newHashMap();
      headers.put(AggregationResultTransport.EVENT_TIME_KEY, elementTimestamp.toString());
      headers.put(
          AggregationResultTransport.AGGREGATION_WINDOW_TIME_KEY, windowEndTimestamp.toString());
      headers.put(AggregationResultTransport.AGGREGATION_VALUE_TIMING_KEY, pane.getTiming().name());
      headers.put(
          AggregationResultTransport.AGGREGATION_VALUE_IS_FINAL_KEY, String.valueOf(pane.isLast()));

      return new BaseAggregationResultTransport<>(kv.getKey(), kv.getValue(), headers)
          .withAggregationName(aggregationName);
    }
  }
}
