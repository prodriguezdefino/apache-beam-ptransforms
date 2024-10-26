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
package com.google.cloud.pso.beam.transforms;

import com.google.cloud.pso.beam.common.transport.CommonTransport;
import com.google.cloud.pso.beam.common.transport.EventTransport;
import com.google.cloud.pso.beam.common.transport.coder.CommonTransportCoder;
import com.google.cloud.pso.beam.options.KafkaOptions;
import com.google.cloud.pso.beam.options.StreamingSourceOptions;
import com.google.cloud.pso.beam.transforms.kafka.ConsumerFactoryFn;
import com.google.cloud.pso.beam.transforms.kafka.KafkaConfig;
import com.google.cloud.pso.beam.transforms.transport.KafkaTransportUtil;
import com.google.cloud.pso.beam.transforms.transport.PubSubLiteTransportUtil;
import com.google.cloud.pso.beam.transforms.transport.PubSubTransport;
import com.google.cloud.pso.beam.transforms.transport.coder.PubSubTransportCoder;
import com.google.cloud.pubsublite.SubscriptionPath;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteIO;
import org.apache.beam.sdk.io.gcp.pubsublite.SubscriberOptions;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.Duration;

public class ReadStreamingSource extends PTransform<PBegin, PCollection<? extends EventTransport>> {

  private static final String NA = "";

  private ReadStreamingSource() {}

  public static ReadStreamingSource create() {
    return new ReadStreamingSource();
  }

  @Override
  public void validate(PipelineOptions options) {
    super.validate(options);
  }

  @Override
  public PCollection<? extends EventTransport> expand(PBegin input) {
    StreamingSourceOptions options =
        input.getPipeline().getOptions().as(StreamingSourceOptions.class);
    return switch (options.getSourceType()) {
      case PUBSUBLITE ->
          input
              .apply(
                  "ReadFromPubSubLite",
                  PubsubLiteIO.read(
                      SubscriberOptions.newBuilder()
                          .setSubscriptionPath(
                              SubscriptionPath.parse(options.getSubscription().get()))
                          .build()))
              .apply(
                  "ConvertIntoPubsubMessages",
                  MapElements.into(TypeDescriptor.of(CommonTransport.class))
                      .via(PubSubLiteTransportUtil.create()))
              .setCoder(CommonTransportCoder.of());
      case PUBSUB ->
          input
              .apply(
                  "ReadFromPubSub",
                  PubsubIO.readMessagesWithAttributesAndMessageId()
                      .fromSubscription(options.getSubscription()))
              .apply(
                  "ConvertIntoTransport",
                  MapElements.into(TypeDescriptor.of(PubSubTransport.class))
                      .via(PubSubTransport.create()))
              .setCoder(PubSubTransportCoder.of());
      case KAFKA ->
          input
              .apply("ReadFromKafka", createKafkaSource(options))
              .apply(
                  "ConvertIntoTransport",
                  MapElements.into(TypeDescriptor.of(CommonTransport.class))
                      .via(KafkaTransportUtil.create()))
              .setCoder(CommonTransportCoder.of());
      default ->
          throw new IllegalArgumentException(
              "Source type " + options.getSourceType() + " not supported.");
    };
  }

  KafkaIO.Read<byte[], byte[]> createKafkaSource(PipelineOptions options) {
    var sourceTopic = options.as(StreamingSourceOptions.class).getInputTopic().get();
    var kafkaOptions = options.as(KafkaOptions.class);
    var kafkaConfig = KafkaConfig.fromOptions(options);

    KafkaIO.Read<byte[], byte[]> source =
        KafkaIO.readBytes()
            // will be overwritten byt the consumer factory fn
            .withBootstrapServers(NA)
            .withTopic(sourceTopic)
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(ByteArrayDeserializer.class)
            .withConsumerConfigUpdates(
                Map.of(
                    "group.id",
                    kafkaConfig.getGroupId(),
                    "bootstrap.servers",
                    kafkaConfig.getBootstrapServers(),
                    "max.partition.fetch.bytes",
                    kafkaConfig.getPartitionMaxFetchSize(),
                    "enable.auto.commit",
                    kafkaConfig.getAutoCommit(),
                    "default.api.timeout.ms",
                    kafkaConfig.getDefaultAPITimeout()))
            .withConsumerFactoryFn(new ConsumerFactoryFn(kafkaOptions));

    if (!kafkaOptions.isKafkaAutocommitEnabled()) {
      source = source.commitOffsetsInFinalize();
    }

    return switch (kafkaOptions.getTimestampType()) {
      case CREATE_TIME -> source.withCreateTime(Duration.standardHours(2));
      case LOG_APPEND_TIME -> source.withLogAppendTime();
      default -> source;
    };
  }
}
