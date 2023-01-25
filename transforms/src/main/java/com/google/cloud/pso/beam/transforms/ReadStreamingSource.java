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
package com.google.cloud.pso.beam.transforms;

import com.google.cloud.pso.beam.common.transport.EventTransport;
import com.google.cloud.pso.beam.transforms.transport.KafkaTransportUtil;
import com.google.cloud.pso.beam.transforms.transport.PubSubTransport;
import com.google.cloud.pso.beam.options.KafkaOptions;
import com.google.cloud.pso.beam.options.StreamingSourceOptions;
import com.google.cloud.pso.beam.transforms.kafka.ConsumerFactoryFn;
import com.google.cloud.pso.beam.transforms.transport.PubSubLiteTransportUtil;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteIO;
import org.apache.beam.sdk.io.gcp.pubsublite.SubscriberOptions;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.Duration;

public class ReadStreamingSource
        extends PTransform<PBegin, PCollection<EventTransport>> {

  private static final String NA = "";

  private ReadStreamingSource() {
  }

  public static ReadStreamingSource create() {
    return new ReadStreamingSource();
  }

  @Override
  public void validate(PipelineOptions options) {
    super.validate(options);
  }

  @Override
  public PCollection<EventTransport> expand(PBegin input) {
    StreamingSourceOptions options
            = input.getPipeline().getOptions().as(StreamingSourceOptions.class);
    PCollection<EventTransport> msgs = null;
    switch (options.getSourceType()) {
      case PUBSUBLITE: {
        var subscriptionPath = SubscriptionPath.parse(options.getSubscription().get());
        msgs = input.apply("ReadFromPubSubLite",
                PubsubLiteIO.read(
                        SubscriberOptions
                                .newBuilder()
                                .setSubscriptionPath(subscriptionPath)
                                .build()))
                .apply("ConvertIntoPubsubMessages",
                        MapElements
                                .into(TypeDescriptor.of(EventTransport.class))
                                .via(PubSubLiteTransportUtil.create()));
        break;
      }
      case PUBSUB: {
        msgs = input.apply("ReadFromPubSub",
                PubsubIO
                        .readMessagesWithAttributesAndMessageId()
                        .fromSubscription(options.getSubscription()))
                .apply("ConvertIntoTransport",
                        MapElements
                                .into(TypeDescriptor.of(EventTransport.class))
                                .via(PubSubTransport.create()));
        break;
      }
      case KAFKA: {
        msgs = input
                .apply("ReadFromKafka", createKafkaSource(options))
                .apply("ConvertIntoSparrowTransport",
                        MapElements
                                .into(TypeDescriptor.of(EventTransport.class))
                                .via(KafkaTransportUtil.create()));
        break;
      }
      default: {
        throw new IllegalArgumentException(
                "Source type " + options.getSourceType() + " not supported.");
      }
    }
    return msgs;
  }

  KafkaIO.Read<byte[], byte[]> createKafkaSource(PipelineOptions options) {
    var sourceTopic = options.as(StreamingSourceOptions.class).getInputTopic().get();
    var kafkaOptions = options.as(KafkaOptions.class);

    KafkaIO.Read<byte[], byte[]> source = KafkaIO
            .readBytes()
            // will be overwritten byt the consumer factory fn
            .withBootstrapServers(NA)
            .withTopic(sourceTopic)
            .withKeyDeserializer(ByteArrayDeserializer.class)
            .withValueDeserializer(ByteArrayDeserializer.class)
            .withConsumerFactoryFn(
                    new ConsumerFactoryFn(kafkaOptions));

    switch (kafkaOptions.getTimestampType()) {
      case CREATE_TIME:
        source = source.withCreateTime(Duration.standardHours(2));
        break;
      case LOG_APPEND_TIME:
        source = source.withLogAppendTime();
        break;
      default:
        break;
    }

    return source;
  }
}
