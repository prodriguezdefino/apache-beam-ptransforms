/*
 * Copyright (C) 2021 Google Inc.
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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Describes the options in use to write a streaming sink.
 */
public interface StreamingSinkOptions extends PipelineOptions {

  @Description("Output topic")
  @Validation.Required
  String getOutputTopic();

  void setOutputTopic(String value);

  @Description("Type of the sink")
  @Default.Enum("PUBSUB")
  SinkType getSinkType();

  void setSinkType(SinkType value);

  public static enum SinkType {
    PUBSUB,
    PUBSUBLITE,
    KAFKA;
  }
}
