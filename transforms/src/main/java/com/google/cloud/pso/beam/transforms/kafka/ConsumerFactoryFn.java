package com.google.cloud.pso.beam.transforms.kafka;

import com.google.cloud.pso.beam.options.KafkaOptions;
import com.google.cloud.pso.beam.options.KafkaSASLSSLOptions;
import java.util.Map;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerFactoryFn
        implements SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {

  private final KafkaConfig kafkaConfig;

  public ConsumerFactoryFn(KafkaOptions options) {
    this.kafkaConfig = options.isKafkaSASLSSLEnabled() == false
            ? options.getKafkaConfig()
            : ((KafkaSASLSSLOptions) options.as(KafkaSASLSSLOptions.class)).getSASLSSLConfig();
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
