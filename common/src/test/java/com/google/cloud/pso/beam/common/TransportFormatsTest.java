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
package com.google.cloud.pso.beam.common;

import com.google.cloud.pso.beam.common.formats.TransportFormats;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.sdk.values.KV;
import org.junit.Assert;
import org.junit.Test;

/** */
public class TransportFormatsTest {
  static Random RANDOM = new Random();

  TransportFormats.AggregationResultValue randomResultValue() {
    return new TransportFormats.AggregationResultValue(
        RandomStringUtils.randomAlphabetic(10)
            + "#"
            + RandomStringUtils.randomNumeric(3)
            + "#"
            + RandomStringUtils.randomAlphabetic(6),
        IntStream.range(0, RANDOM.nextInt(3))
            .mapToObj(i -> KV.of(RandomStringUtils.randomAlphabetic(5), i))
            .collect(Collectors.toMap(KV::getKey, KV::getValue)));
  }

  @Test
  public void testAggregationValueSerializationConsistent() {
    var handler =
        TransportFormats.<TransportFormats.AggregationResultValue>handlerFactory(
                TransportFormats.Format.AGGREGATION_RESULT)
            .apply(null);
    var original = randomResultValue();
    var encoded = handler.encode(original);
    var decoded = handler.decode(encoded);

    Assert.assertEquals(original, decoded);
  }

  @Test
  public void testAggregationValueSerializationMultiThreadedExecution() {
    var handler =
        TransportFormats.<TransportFormats.AggregationResultValue>handlerFactory(
                TransportFormats.Format.AGGREGATION_RESULT)
            .apply(null);

    IntStream.range(0, 1000)
        .parallel()
        .mapToObj(i -> randomResultValue())
        .map(r -> KV.of(r, handler.encode(r)))
        .map(kvEnc -> KV.of(kvEnc.getKey(), handler.decode(kvEnc.getValue())))
        .toList()
        .forEach(kv -> Assert.assertEquals(kv.getKey(), kv.getValue()));
  }
}
