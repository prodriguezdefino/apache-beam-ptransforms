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
import com.google.cloud.pso.beam.common.transport.AggregationResultTransport;
import com.google.cloud.pso.beam.common.transport.CommonTransport;
import com.google.cloud.pso.beam.common.transport.coder.CommonTransportCoder;
import com.google.cloud.pso.beam.generator.thrift.User;
import com.google.cloud.pso.beam.options.AggregationOptions;
import com.google.common.collect.Maps;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

/** Tests for the count aggregation */
public class AggregationsTest {
  private static final Logger LOG = Logger.getLogger(AggregationsTest.class.getName());

  @SuppressWarnings("unchecked")
  CommonTransport createTransport(User baseUser, String uuid, Long startup) {
    var handler =
        TransportFormats.handlerFactory(TransportFormats.Format.THRIFT)
            .apply(baseUser.getClass().getName());
    var emptyHeaders = Maps.<String, String>newHashMap();
    baseUser.setUuid(uuid);
    baseUser.setStartup(startup);
    return new CommonTransport(uuid, emptyHeaders, handler.encode(baseUser));
  }

  private TestStream<CommonTransport> createTestStream() {
    // we need a constant time to have consistent results, in the case of having dynamicly captured
    // time we may fall in the problem of having half of the data in 1 window and half on the other
    // complicating the validation.
    var baseTime = Instant.parse("2007-07-08T05:00:00.000Z");
    var baseUser = new User();

    baseUser.setDescription("Description");
    baseUser.setLocation("Location");

    var rightAfter = baseTime.plus(Duration.standardSeconds(10L));
    var afterAMinuteAndMore = baseTime.plus(Duration.standardSeconds(80L));
    var afterTwoMinutesAndMore = baseTime.plus(Duration.standardSeconds(140L));

    return TestStream.create(CommonTransportCoder.of())
        .advanceWatermarkTo(baseTime)
        .addElements(TimestampedValue.of(createTransport(baseUser, "1", 5L), rightAfter))
        .addElements(TimestampedValue.of(createTransport(baseUser, "2", 3L), rightAfter))
        .addElements(TimestampedValue.of(createTransport(baseUser, "3", 6L), rightAfter))
        .advanceProcessingTime(Duration.standardSeconds(65L)) // force to fire first pane
        .addElements(TimestampedValue.of(createTransport(baseUser, "1", 5L), afterAMinuteAndMore))
        .addElements(TimestampedValue.of(createTransport(baseUser, "3", 10L), afterAMinuteAndMore))
        .advanceProcessingTime(Duration.standardSeconds(65L)) // force to fire a second pane
        .addElements(
            TimestampedValue.of(createTransport(baseUser, "1", 3L), afterTwoMinutesAndMore))
        .advanceProcessingTime(Duration.standardSeconds(65L)) // force to fire a third pane
        .advanceWatermarkToInfinity();
  }

  @SafeVarargs
  private static <Key, Res> Void validateResultsWithEarlyFirings(
      BiConsumer<Res, Res> validateFunction,
      Iterable<AggregationResultTransport<Key, Res>> results,
      KV<String, Res>... expectations) {
    LOG.info("results: " + results.toString());
    Supplier<Stream<AggregationResultTransport<Key, Res>>> validateStream =
        () -> StreamSupport.stream(results.spliterator(), false);

    // we expect 3 final values
    var finalResults = validateStream.get().filter(res -> res.ifFinalValue()).count();
    Assert.assertEquals(3, finalResults);

    // also we expect 9 values, 3 final and 6 early, since we have 3 early firings before
    // completion (the first one with 3 results, the second one with 2 results, and the
    // last one with only 1 result)
    var totalResults = validateStream.get().count();
    Assert.assertEquals(9, totalResults);

    for (var expected : expectations) {
      // check final result for key
      var idFinalResult =
          validateStream
              .get()
              .filter(res -> expected.getKey().equals(res.getAggregationKey()))
              .filter(res -> res.ifFinalValue())
              .findFirst()
              .get()
              .getResult();

      validateFunction.accept(idFinalResult, expected.getValue());
    }

    return null;
  }

  @SafeVarargs
  private static <Key, Res> Void validateResultsWithoutEarlyFirings(
      BiConsumer<Res, Res> validateFunction,
      Iterable<AggregationResultTransport<Key, Res>> results,
      KV<String, Res>... expectations) {
    LOG.info("results: " + results.toString());
    Supplier<Stream<AggregationResultTransport<Key, Res>>> validateStream =
        () -> StreamSupport.stream(results.spliterator(), false);

    // we expect 3 final values
    var finalResults = validateStream.get().filter(res -> res.ifFinalValue()).count();
    Assert.assertEquals(3, finalResults);

    // there are only final values so we expect a total of 3 results
    var totalResults = validateStream.get().count();
    Assert.assertEquals(3, totalResults);

    for (var expected : expectations) {
      // check final result for key
      var idFinalResult =
          validateStream
              .get()
              .filter(res -> expected.getKey().equals(res.getAggregationKey()))
              .filter(res -> res.ifFinalValue())
              .findFirst()
              .get()
              .getResult();

      validateFunction.accept(idFinalResult, expected.getValue());
    }

    return null;
  }

  @Test
  public void testCountAggregation() {
    String[] args = {"--aggregationConfigurationLocation=classpath://count-config.yml"};
    var options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(AggregationOptions.class);
    var configuration = Configuration.AggregationConfigurations.fromOptions(options);
    var testPipeline = TestPipeline.create(options);
    var stream = createTestStream();
    var counted =
        testPipeline
            .apply(stream)
            .apply(CountByFieldsAggregation.create(configuration.configurations().get(0)));

    PAssert.that(counted)
        .satisfies(
            counts ->
                AggregationsTest.<String, Long>validateResultsWithEarlyFirings(
                    (expected, obtained) -> Assert.assertEquals(expected, obtained),
                    counts,
                    KV.of("uuid#1#count", 3L),
                    KV.of("uuid#2#count", 1L),
                    KV.of("uuid#3#count", 2L)));

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSumAggregation() {
    String[] args = {"--aggregationConfigurationLocation=classpath://sum-config.yml"};
    var options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(AggregationOptions.class);
    var configuration = Configuration.AggregationConfigurations.fromOptions(options);
    var testPipeline = TestPipeline.create(options);
    var stream = createTestStream();
    var counted =
        testPipeline
            .apply(stream)
            .apply(SummarizeByFieldsAggregation.sum(configuration.configurations().get(0)));

    PAssert.that(counted)
        .satisfies(
            sums ->
                AggregationsTest.<String, Double>validateResultsWithEarlyFirings(
                    (expected, obtained) -> Assert.assertEquals(expected, obtained, 0.01),
                    sums,
                    KV.of("uuid#1#sum#startup", 13.0),
                    KV.of("uuid#2#sum#startup", 3.0),
                    KV.of("uuid#3#sum#startup", 16.0)));
    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testMinAggregation() {
    String[] args = {"--aggregationConfigurationLocation=classpath://min-config.yml"};
    var options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(AggregationOptions.class);
    var configuration = Configuration.AggregationConfigurations.fromOptions(options);
    var testPipeline = TestPipeline.create(options);
    var stream = createTestStream();
    var counted =
        testPipeline
            .apply(stream)
            .apply(SummarizeByFieldsAggregation.min(configuration.configurations().get(0)));

    PAssert.that(counted)
        .satisfies(
            mins ->
                AggregationsTest.<String, Double>validateResultsWithEarlyFirings(
                    (expected, obtained) -> Assert.assertEquals(expected, obtained, 0.01),
                    mins,
                    KV.of("uuid#1#min#startup", 3.0),
                    KV.of("uuid#2#min#startup", 3.0),
                    KV.of("uuid#3#min#startup", 6.0)));

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testMaxAggregation() {
    String[] args = {"--aggregationConfigurationLocation=classpath://max-config.yml"};
    var options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(AggregationOptions.class);
    var configuration = Configuration.AggregationConfigurations.fromOptions(options);
    var testPipeline = TestPipeline.create(options);
    var stream = createTestStream();
    var counted =
        testPipeline
            .apply(stream)
            .apply(SummarizeByFieldsAggregation.max(configuration.configurations().get(0)));

    PAssert.that(counted)
        .satisfies(
            mins ->
                AggregationsTest.<String, Double>validateResultsWithEarlyFirings(
                    (expected, obtained) -> Assert.assertEquals(expected, obtained, 0.01),
                    mins,
                    KV.of("uuid#1#max#startup", 5.0),
                    KV.of("uuid#2#max#startup", 3.0),
                    KV.of("uuid#3#max#startup", 10.0)));

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testMeanAggregation() {
    String[] args = {"--aggregationConfigurationLocation=classpath://mean-config.yml"};
    var options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(AggregationOptions.class);
    var configuration = Configuration.AggregationConfigurations.fromOptions(options);
    var testPipeline = TestPipeline.create(options);
    var stream = createTestStream();
    var counted =
        testPipeline
            .apply(stream)
            .apply(SummarizeByFieldsAggregation.mean(configuration.configurations().get(0)));

    PAssert.that(counted)
        .satisfies(
            mins ->
                AggregationsTest.<String, Double>validateResultsWithEarlyFirings(
                    (expected, obtained) -> Assert.assertEquals(expected, obtained, 0.01),
                    mins,
                    KV.of("uuid#1#mean#startup", 4.33),
                    KV.of("uuid#2#mean#startup", 3.0),
                    KV.of("uuid#3#mean#startup", 8.0)));

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSimpleConfigurableAggregation() {
    String[] args = {"--aggregationConfigurationLocation=classpath://mean-config.yml"};
    var options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(AggregationOptions.class);
    var testPipeline = TestPipeline.create(options);
    var stream = createTestStream();
    var counted = testPipeline.apply(stream).apply(ConfigurableAggregation.create());

    PAssert.that(counted)
        .satisfies(
            mins ->
                AggregationsTest.validateResultsWithEarlyFirings(
                    (expected, obtained) ->
                        Assert.assertEquals((Double) expected, (Double) obtained, 0.01),
                    mins,
                    KV.of("uuid#1#mean#startup", 4.33),
                    KV.of("uuid#2#mean#startup", 3.0),
                    KV.of("uuid#3#mean#startup", 8.0)));

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testMultipleConfigurableAggregations() {
    // this configuration computes a count per key on 5min windows and then sums those keys on 15min
    // windows, so we expect sums of results => key components are original key + value + "sum" +
    // "result"
    String[] args = {"--aggregationConfigurationLocation=classpath://multiple-config.yml"};
    var options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(AggregationOptions.class);
    var testPipeline = TestPipeline.create(options);
    var stream = createTestStream();
    var counted = testPipeline.apply(stream).apply(ConfigurableAggregation.create());

    PAssert.that(counted)
        .satisfies(
            mins ->
                AggregationsTest.validateResultsWithoutEarlyFirings(
                    (expected, obtained) -> {
                      Assert.assertEquals((Double) expected, (Double) obtained, 0.01);
                    },
                    mins,
                    KV.of("uuid#1#sum#result", 3.0),
                    KV.of("uuid#2#sum#result", 1.0),
                    KV.of("uuid#3#sum#result", 2.0)));

    testPipeline.run().waitUntilFinish();
  }
}
