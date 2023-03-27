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
package com.google.cloud.pso.beam.transforms.kafka;

import com.google.cloud.pso.beam.options.KafkaOptions;
import java.util.Map;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerFactoryFn
    implements SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {

  private final KafkaConfig kafkaConfig;

  public ConsumerFactoryFn(KafkaOptions options) {
    this.kafkaConfig =
        options.isKafkaSASLSSLEnabled() == false
            ? KafkaConfig.fromOptions(options)
            : SASLSSLConfig.fromOptions(options);
  }

  @Override
  public Consumer<byte[], byte[]> apply(Map<String, Object> config) {
    if (this.kafkaConfig.isSecureAccessEnabled) {
      return new KafkaConsumer<>(this.populateCommonOptions(this.prepAuthConfig(config)));
    }
    return new KafkaConsumer<>(this.populateCommonOptions(config));
  }

  private Map<String, Object> prepAuthConfig(Map<String, Object> config) {
    KafkaAuth.setupKeyTabOnWorker(this.kafkaConfig.as(SASLSSLConfig.class));
    KafkaAuth.setupTrustStoreOnWorker(this.kafkaConfig.as(SASLSSLConfig.class));
    return KafkaAuth.populateAuthConfigProperties(config, this.kafkaConfig.as(SASLSSLConfig.class));
  }

  Map<String, Object> populateCommonOptions(Map<String, Object> config) {
    config.put("group.id", this.kafkaConfig.groupId);
    config.put("bootstrap.servers", this.kafkaConfig.bootstrapServers);
    config.put("max.partition.fetch.bytes", this.kafkaConfig.partitionMaxFetchSize);
    config.put("enable.auto.commit", this.kafkaConfig.autoCommit);
    config.put("default.api.timeout.ms", this.kafkaConfig.defaultAPITimeout);
    return config;
  }
}
