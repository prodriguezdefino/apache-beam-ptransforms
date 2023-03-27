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

import com.google.cloud.pso.beam.transforms.kafka.KafkaConfig;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface KafkaOptions extends PipelineOptions {

  enum TimestampType {
    CREATE_TIME,
    LOG_APPEND_TIME,
    PROCESSING_TIME
  }

  @Description("Enables secured access to the Kafka cluster.")
  @Default.Boolean(true)
  Boolean isKafkaSASLSSLEnabled();

  void setKafkaSASLSSLEnabled(Boolean value);

  @Description("Sets the Dataflow containers root folder for keys storage.")
  @Default.String("/tmp/lib/keys")
  String getKeysRootFolder();

  void setKeysRootFolder(String value);

  @Description("Enables secured access to the Kafka cluster.")
  @Default.Boolean(true)
  Boolean isKafkaAutocommitEnabled();

  void setKafkaAutocommitEnabled(Boolean value);

  @Description("The consumer group identifier")
  ValueProvider<String> getBootstrapServers();

  void setBootstrapServers(ValueProvider<String> value);

  @Description("The consumer group identifier")
  ValueProvider<String> getConsumerGroupId();

  void setConsumerGroupId(ValueProvider<String> value);

  @Description("Default API timeout in milliseconds.")
  @Default.Integer(120000)
  Integer getDefaultApiTimeoutMs();

  void setDefaultApiTimeoutMs(Integer value);

  @Description("Default partition max fetch size in bytes.")
  @Default.Integer(0xA00000)
  Integer getPartitionMaxFetchSize();

  void setPartitionMaxFetchSize(Integer value);

  @Description("Retrieves a fully initialized Kafka config object.")
  @Default.InstanceFactory(KafkaConfigFactory.class)
  KafkaConfig getKafkaConfig();

  void setKafkaConfig(KafkaConfig value);

  @Description("Sets the configuration for each record's timestamp.")
  @Default.Enum("LOG_APPEND_TIME")
  TimestampType getTimestampType();

  void setTimestampType(TimestampType value);

  class KafkaConfigFactory implements DefaultValueFactory<KafkaConfig> {

    @Override
    public KafkaConfig create(PipelineOptions options) {
      var opts = options.as(KafkaOptions.class);
      return new KafkaConfig(
          opts.getConsumerGroupId().get(),
          opts.getPartitionMaxFetchSize(),
          opts.isKafkaAutocommitEnabled(),
          opts.getDefaultApiTimeoutMs(),
          opts.isKafkaSASLSSLEnabled(),
          opts.getBootstrapServers().get(),
          opts.getKeysRootFolder());
    }
  }
}
