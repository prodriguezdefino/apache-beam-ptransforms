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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.pso.beam.common.formats.TransportFormats;
import com.google.cloud.pso.beam.common.transport.CommonTransport;
import com.google.cloud.pso.beam.common.transport.coder.CommonTransportCoder;
import com.google.cloud.pso.beam.generator.thrift.User;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;
import com.google.cloud.pso.beam.options.CountByFieldsAggregationOptions;
import com.google.common.collect.Maps;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 *
 */
public class CountAggregationTest {

  @Test
  public void testOptions() throws JsonProcessingException {
    String[] args = {
      "--aggregationKeyNames=uuid"
    };

    var options = PipelineOptionsFactory
            .fromArgs(args).withValidation().as(CountByFieldsAggregationOptions.class);
    var countConfig = options.getCountConfiguration();
    var json = new ObjectMapper().writeValueAsString(countConfig);

    Assert.assertNotNull(json);
    Assert.assertEquals(1, countConfig.getKeyFields().size());
  }

  CommonTransport createTransport(User baseUser, String uuid) {
    var handler = TransportFormats
            .handlerFactory(TransportFormats.Format.THRIFT)
            .apply(baseUser.getClass().getName());
    var emptyHeaders = Maps.<String, String>newHashMap();
    baseUser.setUuid(uuid);
    return new CommonTransport(uuid, emptyHeaders, handler.encode(baseUser));
  }

  @Test
  public void testAggregation() {
    String[] args = {
      "--aggregationKeyNames=uuid",
      "--aggregationWindowInMinutes=15",
      "--aggregationPartialTriggerSeconds=60",
      "--aggregationDiscardPartialResults=false",
      "--aggregationPartialTriggerEventCount=100000",
      "--thriftClassName=com.google.cloud.pso.beam.generator.thrift.User"
    };

    var options = PipelineOptionsFactory
            .fromArgs(args).withValidation().as(CountByFieldsAggregationOptions.class);
    var testPipeline = TestPipeline.create(options);
    var baseTime = Instant.now();
    var baseUser = new User();

    baseUser.setStartup(
            10L);
    baseUser.setDescription(
            "Description");
    baseUser.setLocation(
            "Location");

    var now = Instant.now();
    var afterAMinute = baseTime.plus(Duration.standardMinutes(1L));
    var afterAMinuteAndMore = afterAMinute.plus(Duration.standardSeconds(20L));
    var afterTwoMinutes = afterAMinute.plus(Duration.standardMinutes(1L));
    var afterTwoMinutesAndMore = afterTwoMinutes.plus(Duration.standardSeconds(20L));

    var stream
            = TestStream
                    .create(CommonTransportCoder.of())
                    .advanceWatermarkTo(baseTime)
                    .addElements(TimestampedValue.of(createTransport(baseUser, "1"), now))
                    .addElements(TimestampedValue.of(createTransport(baseUser, "2"), now))
                    .addElements(TimestampedValue.of(createTransport(baseUser, "3"), now))
                    .advanceProcessingTime(Duration.standardSeconds(61L)) // force to fire a pane
                    .advanceWatermarkTo(afterAMinute)
                    .addElements(TimestampedValue.of(createTransport(baseUser, "1"), afterAMinuteAndMore))
                    .addElements(TimestampedValue.of(createTransport(baseUser, "3"), afterAMinuteAndMore))
                    .advanceProcessingTime(Duration.standardSeconds(61L)) // force to fire a pane
                    .advanceWatermarkTo(afterTwoMinutes)
                    .addElements(TimestampedValue.of(createTransport(baseUser, "1"), afterTwoMinutesAndMore))
                    .advanceProcessingTime(Duration.standardSeconds(61L)) // force to fire a pane
                    .advanceWatermarkTo(afterTwoMinutes.plus(Duration.standardMinutes(1L)))
                    .advanceWatermarkToInfinity();

    var counted = testPipeline.apply(stream)
            .apply(CountByFieldsAggregation.create());

    PAssert.that(counted)
            .satisfies(counts -> {
              Supplier<Stream<AggregationResultTransport>> validateStream
                      = () -> StreamSupport.stream(counts.spliterator(), false);

              // we expect 3 final values
              var finalResults = validateStream.get()
                      .filter(res -> res.ifFinalValue())
                      .count();
              Assert.assertEquals(3, finalResults);

              // also we expect 9 values, 3 final and 6 early,
              // since we have 3 early firings before completion (the first one with 3 results,
              // the second one with 2 results, and the last one with only 1 result)
              var totalResults = validateStream.get().count();
              Assert.assertEquals(9, totalResults);

              // final result for uuid=1 is 3L
              var id1FinalResult = (Long) validateStream.get()
                      .filter(res -> res.getAggregationKey().equals("uuid#1#count"))
                      .filter(res -> res.ifFinalValue())
                      .findFirst().get().getResult();

              Assert.assertEquals(3L, id1FinalResult.longValue());

              // final result for uuid=2 is 1L
              var id2FinalResult = (Long) validateStream.get()
                      .filter(res -> res.getAggregationKey().equals("uuid#2#count"))
                      .filter(res -> res.ifFinalValue())
                      .findFirst().get().getResult();

              Assert.assertEquals(1L, id2FinalResult.longValue());

              // final result for uuid=1 is 1L
              var id3FinalResult = (Long) validateStream.get()
                      .filter(res -> res.getAggregationKey().equals("uuid#3#count"))
                      .filter(res -> res.ifFinalValue())
                      .findFirst().get().getResult();

              Assert.assertEquals(2L, id3FinalResult.longValue());

              return null;
            }
            );
    testPipeline.run()
            .waitUntilFinish();
  }

}
