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
import com.google.cloud.pso.beam.common.formats.TransportFormats;
import com.google.cloud.pso.beam.common.formats.options.TransportFormatOptions;
import com.google.cloud.pso.beam.common.transport.AggregationResultTransport;
import com.google.cloud.pso.beam.common.transport.AggregationTransport;
import com.google.cloud.pso.beam.common.transport.EventTransport;
import com.google.cloud.pso.beam.common.transport.Transport;
import com.google.cloud.pso.beam.options.AggregationOptions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
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
  protected static final SerializableFunction<
          TransportFormatConfiguration, TransportFormats.Handler>
      FORMAT_HANDLER_FUNC =
          configuration -> {
            var format = configuration.format();
            var formatHandlerFactory = TransportFormats.handlerFactory(format);

            return switch (format) {
              case THRIFT -> formatHandlerFactory.apply(configuration.thriftClassName());
              case AVRO -> formatHandlerFactory.apply(configuration.avroSchemaLocation());
            };
          };

  /** A function that decodes a raw bytes array given the expected transport configuration. */
  static final SerializableBiFunction<TransportFormatConfiguration, byte[], Object> EVENT_DECODER =
      (configuration, data) -> {
        return FORMAT_HANDLER_FUNC.apply(configuration).decode(data);
      };

  /**
   * The coder needed for the aggregation key given the expected type.
   *
   * @return A coder for the aggregation's key
   */
  protected abstract Coder<Key> keyCoder();

  /**
   * The coder needed for the aggregation values given the expected type.
   *
   * @return A coder for the aggregation's values
   */
  protected abstract Coder<Res> resultCoder();

  /**
   * Creates a function that given a list of field names and the decoded transport object, returns
   * the key expected for the aggregation.
   *
   * @param config The transport format configuration
   * @return The key extraction function.
   */
  protected abstract SerializableBiFunction<List<String>, Object, Key> keyExtractorFunction(
      TransportFormatConfiguration config);

  /**
   * Creates a function that given a list of field names and the decoded transport object, returns a
   * map of the desired values mapped by their name.
   *
   * @param config The transport format configuration
   * @return The mapped values extraction function.
   */
  protected abstract SerializableBiFunction<List<String>, Object, Map<String, Value>>
      valuesExtractorFunction(TransportFormatConfiguration config);

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

  /**
   * Returns the transport format configuration given the provided pipeline options.
   *
   * @param options the pipeline's options
   * @return a transport format configuration
   */
  protected TransportFormatConfiguration transportFormatConfigurationFunction(
      PipelineOptions options) {
    return TransportFormatConfiguration.fromOptions(options.as(TransportFormatOptions.class));
  }

  @Override
  public PCollection<AggregationResultTransport<Key, Res>> expand(
      PCollection<? extends Transport> input) {
    var options = input.getPipeline().getOptions();
    var windowConfig =
        AggregationWindowConfiguration.fromOptions(options.as(AggregationOptions.class));
    var transportConfig = transportFormatConfigurationFunction(options);
    var aggDataConfig =
        AggregationDataConfiguration.fromOptions(options.as(AggregationOptions.class));
    var aggTransportCoder =
        BaseAggregationTransportCoder.of(
            Functions.curry(EVENT_DECODER).apply(transportConfig),
            Functions.curry(keyExtractorFunction(transportConfig)).apply(aggDataConfig.keyFields()),
            Functions.curry(valuesExtractorFunction(transportConfig))
                .apply(aggDataConfig.valueFields()));
    var aggResultTransportCoder = AggregationResultTransportCoder.of(keyCoder(), resultCoder());

    return input
        .apply(
            "ToAggregationTransport",
            ParDo.of(
                new ToAggregationTransports<>(
                    keyExtractorFunction(transportConfig),
                    valuesExtractorFunction(transportConfig),
                    transportConfig,
                    aggDataConfig)))
        .setCoder(aggTransportCoder)
        .apply("MapToKV", transportMapper())
        .apply("Flat", Flatten.iterables())
        .apply("Window", createWindow(windowConfig))
        .apply("CountEventsPerKey", aggregation())
        .apply("ToAggregationResults", ParDo.of(new ToAggregationResults<>()))
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
   * @param triggerSeconds when using early firings, the max amount of time that will be waiting
   *     before outputting data, if the element count was not already triggered before
   * @return the trigger behavior for the aggregation.
   */
  protected Trigger createTrigger(
      Boolean earlyFiring, Integer elementCount, Integer triggerSeconds) {
    Trigger trigger = null;
    if (earlyFiring) {
      trigger =
          AfterWatermark.pastEndOfWindow()
              .withEarlyFirings(
                  AfterFirst.of(
                      AfterPane.elementCountAtLeast(elementCount),
                      AfterProcessingTime.pastFirstElementInPane()
                          .plusDelayOf(Duration.standardSeconds(triggerSeconds))));
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
  protected Window<KV<Key, Res>> createWindow(AggregationWindowConfiguration configuration) {
    var window =
        Window.<KV<Key, Res>>into(
                FixedWindows.of(Duration.standardMinutes(configuration.windowLengthInMinutes())))
            .triggering(
                createTrigger(
                    configuration.windowEarlyFirings(),
                    configuration.partialResultElementCountTrigger(),
                    configuration.partialResultTimeInSeconds()));

    if (configuration.allowedLatenessInMinutes() > 0)
      window =
          window.withAllowedLateness(
              Duration.standardMinutes(configuration.allowedLatenessInMinutes()),
              Window.ClosingBehavior.FIRE_ALWAYS);
    if (configuration.discardPartialResults()) {
      window = window.discardingFiredPanes();
    } else {
      window = window.accumulatingFiredPanes();
    }
    return window;
  }

  /** The window configuration record. */
  public record AggregationWindowConfiguration(
      Integer windowLengthInMinutes,
      Boolean windowEarlyFirings,
      Integer allowedLatenessInMinutes,
      Boolean discardPartialResults,
      Integer partialResultElementCountTrigger,
      Integer partialResultTimeInSeconds) {

    static AggregationWindowConfiguration fromOptions(AggregationOptions opts) {
      return new AggregationWindowConfiguration(
          opts.getAggregationWindowInMinutes(),
          opts.getAggregationEarlyFirings(),
          opts.getAggregationAllowedLatenessInMinutes(),
          opts.getAggregationDiscardPartialResults(),
          opts.getAggregationPartialTriggerEventCount(),
          opts.getAggregationPartialTriggerSeconds());
    }
  }

  /**
   * The transport format configuration. Indicates the format which defines what configuration to
   * use, a class name in the case of Thrift or an Avro schema location if Avro is the format.
   */
  public record TransportFormatConfiguration(
      TransportFormats.Format format, String thriftClassName, String avroSchemaLocation)
      implements Serializable {
    static TransportFormatConfiguration fromOptions(TransportFormatOptions opts) {
      return new TransportFormatConfiguration(
          opts.getTransportFormat(), opts.getThriftClassName(), opts.getAvroSchemaLocation());
    }
  }

  /** The configuration for the data to be aggregated. */
  public record AggregationDataConfiguration(List<String> keyFields, List<String> valueFields)
      implements Serializable {
    static AggregationDataConfiguration fromOptions(AggregationOptions opts) {
      return new AggregationDataConfiguration(
          Arrays.asList(opts.getAggregationKeyNames().split(",")),
          Arrays.asList(opts.getAggregationValueNames().split(",")));
    }
  }

  static class ToAggregationTransports<Key, Value>
      extends DoFn<Transport, BaseAggregationTransport<Key, Value>> {

    private final SerializableBiFunction<List<String>, Object, Key> keyExtractor;
    private final SerializableBiFunction<List<String>, Object, Map<String, Value>> valuesExtractor;
    private final TransportFormatConfiguration transportConfig;
    private final AggregationDataConfiguration dataConfig;

    public ToAggregationTransports(
        SerializableBiFunction<List<String>, Object, Key> keyExtractor,
        SerializableBiFunction<List<String>, Object, Map<String, Value>> valuesExtractor,
        TransportFormatConfiguration transportConfig,
        AggregationDataConfiguration dataConfig) {
      this.keyExtractor = keyExtractor;
      this.valuesExtractor = valuesExtractor;
      this.transportConfig = transportConfig;
      this.dataConfig = dataConfig;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(
          BaseAggregationTransport.fromTransportAndConfigurationAndExtractors(
              context.element(), transportConfig, dataConfig, keyExtractor, valuesExtractor));
    }
  }

  static class ToAggregationResults<Key, Res>
      extends DoFn<KV<Key, Res>, AggregationResultTransport<Key, Res>> {

    @ProcessElement
    public void processElement(ProcessContext context, BoundedWindow window, PaneInfo pane) {
      context.output(
          BaseAggregationResultTransport.fromKV(
              context.element(), Instant.now(), window.maxTimestamp(), pane));
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

    private final SerializableFunction<byte[], Object> dataDecoder;
    private final SerializableFunction<Object, Key> keyExtractor;
    private final SerializableFunction<Object, Map<String, Value>> valuesExtractor;

    public BaseAggregationTransportCoder(
        SerializableFunction<byte[], Object> dataDecoder,
        SerializableFunction<Object, Key> keyExtractor,
        SerializableFunction<Object, Map<String, Value>> valuesExtractor) {
      this.dataDecoder = dataDecoder;
      this.keyExtractor = keyExtractor;
      this.valuesExtractor = valuesExtractor;
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
          id, headers, data, dataDecoder, keyExtractor, valuesExtractor);
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

    public static <Key, Value>
        BaseAggregationTransport<Key, Value> fromTransportAndConfigurationAndExtractors(
            Transport transport,
            TransportFormatConfiguration transportConfig,
            AggregationDataConfiguration aggDataConfig,
            SerializableBiFunction<List<String>, Object, Key> keyByFieldsExtractor,
            SerializableBiFunction<List<String>, Object, Map<String, Value>>
                valuesByFieldsExtractor) {

      if (transport instanceof EventTransport ev) {
        var curriedDecoder = Functions.curry(EVENT_DECODER).apply(transportConfig);
        var curriedKeyExtractor =
            Functions.curry(keyByFieldsExtractor).apply(aggDataConfig.keyFields());
        var curriedValuesExtractor =
            Functions.curry(valuesByFieldsExtractor).apply(aggDataConfig.valueFields());

        return new BaseAggregationTransport<>(
            ev.getId(),
            ev.getHeaders(),
            ev.getData(),
            curriedDecoder,
            curriedKeyExtractor,
            curriedValuesExtractor);
      } else if (transport instanceof AggregationResultTransport agg) {
        SerializableFunction<byte[], Object> decoder = data -> agg.getResult();
        @SuppressWarnings("unchecked")
        SerializableFunction<Object, Key> keyExtractor = data -> (Key) agg.getAggregationKey();
        @SuppressWarnings("unchecked")
        SerializableFunction<Object, Map<String, Value>> valuesExtractor =
            data -> Map.of("result", (Value) agg.getResult());
        return new BaseAggregationTransport<>(
            agg.getId(), agg.getHeaders(), null, decoder, keyExtractor, valuesExtractor);
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
        KV<Key, Res> kv, Instant elementTimestamp, Instant windowEndTimestamp, PaneInfo pane) {
      var headers = Maps.<String, String>newHashMap();
      headers.put(AggregationResultTransport.EVENT_TIME_KEY, elementTimestamp.toString());
      headers.put(
          AggregationResultTransport.AGGREGATION_WINDOW_TIME_KEY, windowEndTimestamp.toString());
      headers.put(AggregationResultTransport.AGGREGATION_VALUE_TIMING_KEY, pane.getTiming().name());
      headers.put(
          AggregationResultTransport.AGGREGATION_VALUE_IS_FINAL_KEY, String.valueOf(pane.isLast()));

      return new BaseAggregationResultTransport<>(kv.getKey(), kv.getValue(), headers)
          .withAggregationName("count");
    }
  }
}
