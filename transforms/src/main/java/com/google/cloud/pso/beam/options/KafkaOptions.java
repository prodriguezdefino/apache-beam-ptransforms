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

import com.google.cloud.pso.beam.transforms.kafka.KafkaConfig;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface KafkaOptions
        extends PipelineOptions {

  enum TimestampType {
    CREATE_TIME,
    LOG_APPEND_TIME,
    PROCESSING_TIME
  }

  @Description(value = "Enables secured access to the Kafka cluster.")
  @Default.Boolean(value = true)
  Boolean isKafkaSASLSSLEnabled();

  void setKafkaSASLSSLEnabled(Boolean var1);

  @Description(value = "Sets the Dataflow containers root folder for keys storage.")
  @Default.String(value = "/tmp/lib/keys")
  String getKeysRootFolder();

  void setKeysRootFolder(String var1);

  @Description(value = "Enables secured access to the Kafka cluster.")
  @Default.Boolean(value = true)
  Boolean isKafkaAutocommitEnabled();

  void setKafkaAutocommitEnabled(Boolean var1);

  @Description(value = "The consumer group identifier")
  ValueProvider<String> getBootstrapServers();

  void setBootstrapServers(ValueProvider<String> var1);

  @Description(value = "The consumer group identifier")
  ValueProvider<String> getConsumerGroupId();

  void setConsumerGroupId(ValueProvider<String> var1);

  @Description(value = "Default API timeout in milliseconds.")
  @Default.Integer(value = 120000)
  Integer getDefaultApiTimeoutMs();

  void setDefaultApiTimeoutMs(Integer var1);

  @Description(value = "Default partition max fetch size in bytes.")
  @Default.Integer(value = 0xA00000)
  Integer getPartitionMaxFetchSize();

  void setPartitionMaxFetchSize(Integer var1);

  @Description(value = "The project where SecretManager is storing the needed materials.")
  ValueProvider<String> getSecretManagerProjectId();

  void setSecretManagerProjectId(ValueProvider<String> var1);

  @Description(value = "Retrieves a fully initialized Kafka config object.")
  @Default.InstanceFactory(value = KafkaConfigFactory.class)
  KafkaConfig getKafkaConfig();

  void setKafkaConfig(KafkaConfig var1);

  @Description(value = "Sets the configuration for each record's timestamp.")
  @Default.Enum("LOG_APPEND_TIME")
  TimestampType getTimestampType();

  void setTimestampType(TimestampType var1);

  static class KafkaConfigFactory
          implements DefaultValueFactory<KafkaConfig> {

    public KafkaConfig create(PipelineOptions options) {
      var opts = options.as(KafkaOptions.class);
      return new KafkaConfig(
              opts.getConsumerGroupId().get(),
              opts.getPartitionMaxFetchSize(),
              opts.isKafkaAutocommitEnabled(),
              opts.getDefaultApiTimeoutMs(),
              opts.isKafkaSASLSSLEnabled(),
              opts.getSecretManagerProjectId().get(),
              opts.getBootstrapServers().get(),
              opts.getKeysRootFolder());
    }
  }
}
