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
package com.google.cloud.pso.beam.options;

import com.google.cloud.pso.beam.common.formats.options.TransportFormatOptions;
import com.google.cloud.pso.beam.transforms.aggregations.CountByFieldsAggregationConfiguration;
import java.util.Arrays;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Defines the options to configure when running a count aggregation.
 */
public interface CountByFieldsAggregationOptions extends TransportFormatOptions {

  @Description("The aggregation key field names (it can be a comma separated value list).")
  @Validation.Required
  String getAggregationKeyNames();

  void setAggregationKeyNames(String value);

  @Description("Retrieves a fully initialized Count configuration object.")
  @Default.InstanceFactory(CountConfigurationFactory.class)
  CountByFieldsAggregationConfiguration getCountConfiguration();

  void setCountConfiguration(CountByFieldsAggregationConfiguration value);

  static class CountConfigurationFactory
          implements DefaultValueFactory<CountByFieldsAggregationConfiguration> {

    @Override
    public CountByFieldsAggregationConfiguration create(PipelineOptions options) {
      var opts = options.as(CountByFieldsAggregationOptions.class);
      return new CountByFieldsAggregationConfiguration(
              opts.getTransportFormat(),
              opts.getThriftClassName(),
              opts.getAvroSchemaLocation(),
              Arrays.asList(opts.getAggregationKeyNames().split(",")));
    }
  }
}
