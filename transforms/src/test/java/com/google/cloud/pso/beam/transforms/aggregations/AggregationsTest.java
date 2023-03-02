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
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
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

  @Test
  public void testCountAggregation() {
    String[] args = {
      "--aggregationKeyNames=uuid",
      "--aggregationWindowInMinutes=15",
      "--aggregationPartialTriggerSeconds=60",
      "--aggregationDiscardPartialResults=false",
      "--aggregationPartialTriggerEventCount=100000",
      "--thriftClassName=com.google.cloud.pso.beam.generator.thrift.User"
    };

    var options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(AggregationOptions.class);
    var testPipeline = TestPipeline.create(options);
    var stream = createTestStream();
    var counted = testPipeline.apply(stream).apply(CountByFieldsAggregation.create());

    PAssert.that(counted)
        .satisfies(
            counts -> {
              LOG.info("results: " + counts.toString());
              Supplier<Stream<AggregationResultTransport<String, Long>>> validateStream =
                  () -> StreamSupport.stream(counts.spliterator(), false);

              // we expect 3 final values
              var finalResults = validateStream.get().filter(res -> res.ifFinalValue()).count();
              Assert.assertEquals(3, finalResults);

              // also we expect 9 values, 3 final and 6 early, since we have 3 early firings before
              // completion (the first one with 3 results, the second one with 2 results, and the
              // last one with only 1 result)
              var totalResults = validateStream.get().count();
              Assert.assertEquals(9, totalResults);

              // final result for uuid=1 is 3L
              var id1FinalResult =
                  validateStream
                      .get()
                      .filter(res -> "uuid#1#count".equals(res.getAggregationKey()))
                      .filter(res -> res.ifFinalValue())
                      .findFirst()
                      .get()
                      .getResult();

              Assert.assertEquals(3L, id1FinalResult.longValue());

              // final result for uuid=2 is 1L
              var id2FinalResult =
                  validateStream
                      .get()
                      .filter(res -> "uuid#2#count".equals(res.getAggregationKey()))
                      .filter(res -> res.ifFinalValue())
                      .findFirst()
                      .get()
                      .getResult();

              Assert.assertEquals(1L, id2FinalResult.longValue());

              // final result for uuid=1 is 1L
              var id3FinalResult =
                  validateStream
                      .get()
                      .filter(res -> "uuid#3#count".equals(res.getAggregationKey()))
                      .filter(res -> res.ifFinalValue())
                      .findFirst()
                      .get()
                      .getResult();

              Assert.assertEquals(2L, id3FinalResult.longValue());

              return null;
            });
    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSumAggregation() {
    String[] args = {
      "--aggregationKeyNames=uuid",
      "--aggregationValueNames=startup",
      "--aggregationWindowInMinutes=15",
      "--aggregationEarlyFirings=true",
      "--aggregationPartialTriggerSeconds=60",
      "--aggregationDiscardPartialResults=false",
      "--aggregationPartialTriggerEventCount=100000",
      "--thriftClassName=com.google.cloud.pso.beam.generator.thrift.User"
    };

    var options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(AggregationOptions.class);
    var testPipeline = TestPipeline.create(options);
    var stream = createTestStream();
    var counted = testPipeline.apply(stream).apply(SumByFieldsAggregation.create());

    PAssert.that(counted)
        .satisfies(
            counts -> {
              LOG.info("results: " + counts.toString());
              Supplier<Stream<AggregationResultTransport<String, Double>>> validateStream =
                  () -> StreamSupport.stream(counts.spliterator(), false);

              // we expect 3 final values
              var finalResults = validateStream.get().filter(res -> res.ifFinalValue()).count();
              Assert.assertEquals(3, finalResults);

              // also we expect 9 values, 3 final and 6 early, since we have 3 early firings before
              // completion (the first one with 3 results, the second one with 2 results, and the
              // last one with only 1 result)
              var totalResults = validateStream.get().count();
              Assert.assertEquals(9, totalResults);

              // final result for uuid=1 is 13.0
              var id1FinalResult =
                  validateStream
                      .get()
                      .filter(res -> "uuid#1#startup".equals(res.getAggregationKey()))
                      .filter(res -> res.ifFinalValue())
                      .findFirst()
                      .get()
                      .getResult();

              Assert.assertEquals(13.0, id1FinalResult, 0.01);

              // final result for uuid=2 is 3.0
              var id2FinalResult =
                  validateStream
                      .get()
                      .filter(res -> "uuid#2#startup".equals(res.getAggregationKey()))
                      .filter(res -> res.ifFinalValue())
                      .findFirst()
                      .get()
                      .getResult();

              Assert.assertEquals(3.0, id2FinalResult, 0.01);

              // final result for uuid=1 is 16.0
              var id3FinalResult =
                  validateStream
                      .get()
                      .filter(res -> "uuid#3#startup".equals(res.getAggregationKey()))
                      .filter(res -> res.ifFinalValue())
                      .findFirst()
                      .get()
                      .getResult();

              Assert.assertEquals(16.0, id3FinalResult, 0.01);

              return null;
            });
    testPipeline.run().waitUntilFinish();
  }
}
