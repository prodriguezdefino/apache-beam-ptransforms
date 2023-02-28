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
package com.google.cloud.pso.beam.common.transport;

import com.google.cloud.pso.beam.common.Utilities;
import java.util.Optional;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.Instant;

/**
 * Represents the result of an aggregation as a transport.
 *
 * @param <K> the type of the aggregation key
 * @param <V> the type of the aggregation result
 */
public non-sealed interface AggregationResultTransport<K, V> extends Transport {

  public enum ResultType {
    INT,
    LONG,
    DOUBLE,
    FLOAT,
    STRING
  }

  public static final String AGGREGATION_NAME_KEY = "aggregationName";
  public static final String DEFAULT_AGGREGATION_NAME = "defaultAggregationName";
  public static final String AGGREGATION_WINDOW_TIME_KEY = "aggregationWindowMaxTimestamp";
  public static final String AGGREGATION_VALUE_TIMING_KEY = "aggregationValueTiming";
  public static final String AGGREGATION_VALUE_IS_FINAL_KEY = "aggregationValueIsFinal";

  K getAggregationKey();

  V getResult();

  ResultType getType();

  default AggregationResultTransport<K, V> withAggregationName(String name) {
    this.getHeaders().put(AGGREGATION_NAME_KEY, name);
    return this;
  }

  default String getAggregationName() {
    return Optional.ofNullable(this.getHeaders().get(AGGREGATION_NAME_KEY))
        .orElse(DEFAULT_AGGREGATION_NAME);
  }

  default Optional<String> getAggregationTimestamp() {
    return Optional.ofNullable(this.getHeaders().get(EVENT_TIME_KEY))
        .map(strTs -> Instant.parse(strTs))
        .map(Utilities::formatHourGranularityTimestamp)
        .or(() -> Optional.empty());
  }

  default Optional<String> getAggregationWindowTimestamp() {
    return Optional.ofNullable(this.getHeaders().get(AGGREGATION_WINDOW_TIME_KEY))
        .map(strTs -> Instant.parse(strTs))
        .map(Utilities::formatMinuteGranularityTimestamp)
        .or(() -> Optional.empty());
  }

  default PaneInfo.Timing getAggregationTiming() {
    return Optional.ofNullable(this.getHeaders().get(AGGREGATION_VALUE_TIMING_KEY))
        .map(PaneInfo.Timing::valueOf)
        .orElse(PaneInfo.Timing.UNKNOWN);
  }

  default Boolean ifFinalValue() {
    return Optional.ofNullable(this.getHeaders().get(AGGREGATION_VALUE_IS_FINAL_KEY))
        .map(Boolean::valueOf)
        .orElse(false);
  }

  default String asString() {
    return new StringBuilder()
        .append("aggregationKey")
        .append("=")
        .append(getAggregationKey().toString())
        .append(",")
        .append("result")
        .append("=")
        .append(getResult().toString())
        .append(",")
        .append("timestamp")
        .append("=")
        .append(getAggregationTimestamp())
        .append(",")
        .append("headers")
        .append("=")
        .append(getHeaders().toString())
        .toString();
  }
}
