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
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/** Defines the options to configure when running a count aggregation. */
public interface AggregationOptions extends TransportFormatOptions {

  @Description("The aggregation key field names (it can be a comma separated value list).")
  @Validation.Required
  String getAggregationKeyNames();

  void setAggregationKeyNames(String value);

  @Description("The aggregation value field names (it can be a comma separated value list).")
  @Default.String("")
  String getAggregationValueNames();

  void setAggregationValueNames(String value);

  @Description("The amount of seconds the aggregation timer will wait to trigger.")
  @Default.Integer(30)
  Integer getAggregationPartialTriggerSeconds();

  void setAggregationPartialTriggerSeconds(Integer value);

  @Description("The amount of events the aggregation timer will count to trigger.")
  @Default.Integer(1000)
  Integer getAggregationPartialTriggerEventCount();

  void setAggregationPartialTriggerEventCount(Integer value);

  @Description("Configures if the aggregation should accumulate or discard partial results.")
  @Default.Boolean(false)
  Boolean getAggregationDiscardPartialResults();

  void setAggregationDiscardPartialResults(Boolean value);

  @Description("Configures if the aggregation should trigger early results.")
  @Default.Boolean(true)
  Boolean getAggregationEarlyFirings();

  void setAggregationEarlyFirings(Boolean value);

  @Description("Configures how much time in minutes the aggregation will wait for late data.")
  @Default.Integer(1)
  Integer getAggregationAllowedLatenessInMinutes();

  void setAggregationAllowedLatenessInMinutes(Integer value);

  @Description("Configures how much time the aggregation window last.")
  @Default.Integer(1)
  Integer getAggregationWindowInMinutes();

  void setAggregationWindowInMinutes(Integer value);
}
