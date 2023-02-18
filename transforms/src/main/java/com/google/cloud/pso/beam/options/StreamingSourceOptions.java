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
import org.apache.beam.sdk.options.ValueProvider;

/**
 * Describes the options in use to read from a streaming source.
 */
public interface StreamingSourceOptions
        extends PipelineOptions {

  @Description("The topic to read from. In the case of PubSub or PubSubLite "
          + "this is merely informative. In the case of Kafka, this is the topic name "
          + "that will be used to connect.")
  @Validation.Required
  ValueProvider<String> getInputTopic();

  void setInputTopic(ValueProvider<String> var1);

  @Description("The subscription to use. "
          + "In the case of PubSub and PubSubLite this is the actual identifier for the "
          + "subscription that will be used to read. In the case of Kafka this should contain "
          + "the bootstrap server configuration (this can be as the expected list of "
          + "comma separated server).")
  @Validation.Required
  ValueProvider<String> getSubscription();

  void setSubscription(ValueProvider<String> var1);

  @Description("Selects the source type to read from.")
  @Default.Enum("PUBSUB")
  SourceType getSourceType();

  void setSourceType(SourceType var1);

  enum SourceType {
    PUBSUB,
    PUBSUBLITE,
    KAFKA;
  }
}
