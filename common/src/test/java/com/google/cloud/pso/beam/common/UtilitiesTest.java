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

import static org.junit.Assert.*;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.joda.time.Instant;
import org.junit.Test;

/** */
public class UtilitiesTest {

  public UtilitiesTest() {}

  @Test
  public void testSkew() {
    final Integer totalPartitions = 1000;
    Integer onePerc = Double.valueOf(totalPartitions.doubleValue() * 0.01).intValue();
    Long totalElements = 10000000L;
    Supplier<Integer> numberGenerator =
        () -> {
          return Utilities.nextSkewedBoundedInteger(0, totalPartitions, 80, 0);
        };

    var results =
        LongStream.range(0, totalElements)
            .mapToObj(i -> numberGenerator.get())
            .collect(Collectors.groupingBy(a -> a, Collectors.summingInt(b -> 1)))
            .entrySet();

    var ordered =
        results.stream()
            .sorted((a, b) -> -(a.getValue().compareTo(b.getValue())))
            .collect(Collectors.toList());

    var totalquantity = ordered.stream().map(Map.Entry::getValue).reduce((a, b) -> a + b);
    // validate all the counts add to total elements
    assertTrue(totalElements == totalquantity.orElse(-1).longValue());

    // take the highest 1% given the bucket values and sum them, it should be 80% of total volume
    var quantity = ordered.stream().limit(onePerc).map(Map.Entry::getValue).reduce((a, b) -> a + b);
    Double onePercentIsWhatOfTotal = quantity.orElse(-1).doubleValue() * 100 / totalElements;
    assertTrue(onePercentIsWhatOfTotal > 80);
    assertTrue(onePercentIsWhatOfTotal < 90);
  }

  @Test
  public void testFormat() {
    var now = Instant.now();
    var formatted = Utilities.formatMinuteGranularityTimestamp(now);
    assertNotNull(formatted);
  }
}
