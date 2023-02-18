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

import com.google.cloud.pso.beam.common.transport.EventTransport;
import static com.google.cloud.pso.beam.common.transport.EventTransport.EVENT_TIME_PROPERTY_NAME;
import java.util.Optional;

/**
 * Represents the result of an aggregation as a transport.
 *
 * @param <K> the type of the aggregation key
 * @param <V> the type of the aggregation result
 */
public interface AggregationResultTransport<K, V> extends EventTransport {

  static final String AGGREGATION_NAME_KEY = "aggregationName";
  static final String DEFAULT_AGGREGATION_NAME = "defaultAggregationName";

  K getAggregationKey();

  V getResult();

  default AggregationResultTransport<K, V> withAggregationName(String name) {
    this.getHeaders().put(AGGREGATION_NAME_KEY, name);
    return this;
  }

  default String getAggregationName() {
    return Optional
            .ofNullable(this.getHeaders().get(EVENT_TIME_PROPERTY_NAME))
            .orElse(DEFAULT_AGGREGATION_NAME);
  }

}
