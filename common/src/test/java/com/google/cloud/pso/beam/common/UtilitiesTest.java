package com.google.cloud.pso.beam.common;

import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 */
public class UtilitiesTest {

  public UtilitiesTest() {
  }

  @Test
  public void testSkew() {
    final Integer totalPartitions = 1000;
    Integer onePerc = Double.valueOf(totalPartitions.doubleValue() * 0.01).intValue();
    Long totalElements = 10000000L;
    Supplier<Integer> numberGenerator = () -> {
      return Utilities.nextSkewedBoundedInteger(0, totalPartitions, 80, 0);
    };

    var results = LongStream
            .range(0, totalElements)
            .mapToObj(i -> numberGenerator.get())
            .collect(Collectors.groupingBy(a -> a, Collectors.summingInt(b -> 1)))
            .entrySet();

    var ordered = results
            .stream()
            .sorted((a, b) -> -(a.getValue().compareTo(b.getValue())))
            .collect(Collectors.toList());

    var totalquantity = ordered
            .stream()
            .map(Map.Entry::getValue)
            .reduce((a, b) -> a + b);
    // validate all the counts add to total elements
    assertTrue(totalElements == totalquantity.orElse(-1).longValue());

    // take the highest 1% given the bucket values and sum them, it should be 80% of total volume
    var quantity = ordered
            .stream()
            .limit(onePerc)
            .map(Map.Entry::getValue)
            .reduce((a, b) -> a + b);
    Double onePercentIsWhatOfTotal = quantity.orElse(-1).doubleValue() * 100 / totalElements;
    assertTrue(onePercentIsWhatOfTotal > 80);
    assertTrue(onePercentIsWhatOfTotal < 90);

  }
}
