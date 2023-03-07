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

import com.google.cloud.pso.beam.common.transport.AggregationResultTransport;
import com.google.cloud.pso.beam.common.transport.Transport;
import com.google.cloud.pso.beam.options.AggregationOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

/**
 * This PTransform acts as the entry point for all the aggregations that can be configured through
 * the YAML configuration file provided through the AggregationOption interface.
 *
 * @param <Key> The type of the key used for the aggregation
 * @param <Value> The type of the values to be aggregated
 * @param <Res> The result type for the aggregation
 */
public class ConfigurableAggregation<Key, Value, Res>
    extends PTransform<
        PCollection<? extends Transport>, PCollection<AggregationResultTransport<Key, Res>>> {

  ConfigurableAggregation() {}

  /**
   * Creates a configurable aggregation.
   *
   * @param <Key> The type of the key used for the aggregation
   * @param <Value> The type of the values to be aggregated
   * @param <Res> The result type for the aggregation
   * @return the configurable aggregation instance, configured through the YAML file provided as
   *     part of the AggregationOptions interface.
   */
  public static <Key, Value, Res> ConfigurableAggregation<Key, Value, Res> create() {
    return new ConfigurableAggregation<>();
  }

  private BaseAggregation createAggregation(Configuration.AggregationConfiguration configuration) {
    if (configuration instanceof Configuration.Count count) {
      return CountByFieldsAggregation.create(count);
    } else if (configuration instanceof Configuration.Max max) {
      return SummarizeByFieldsAggregation.max(max);
    } else if (configuration instanceof Configuration.Min min) {
      return SummarizeByFieldsAggregation.min(min);
    } else if (configuration instanceof Configuration.Mean mean) {
      return SummarizeByFieldsAggregation.mean(mean);
    } else if (configuration instanceof Configuration.Sum sum) {
      return SummarizeByFieldsAggregation.sum(sum);
    } else
      throw new IllegalArgumentException(
          "The aggregation type configured can not be created: " + configuration);
  }

  @SuppressWarnings("unchecked")
  @Override
  public PCollection<AggregationResultTransport<Key, Res>> expand(
      PCollection<? extends Transport> input) {
    var options = input.getPipeline().getOptions().as(AggregationOptions.class);
    var aggsConfig = Configuration.AggregationConfigurations.fromOptions(options);
    var aggs =
        aggsConfig.configurations().stream().map(config -> createAggregation(config)).toList();
    PCollection<AggregationResultTransport<Key, Res>> res = null;

    for (BaseAggregation<Key, Value, Res> agg : aggs) {
      if (res == null) {
        res = input.apply(agg.getName(), agg);
      } else {
        res = res.apply(agg.getName(), agg);
      }
    }

    return res;
  }
}
