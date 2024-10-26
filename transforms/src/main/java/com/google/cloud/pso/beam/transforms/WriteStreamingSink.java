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

import com.google.cloud.pso.beam.common.transport.EventTransport;
import com.google.cloud.pso.beam.options.StreamingSinkOptions.SinkType;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.proto.AttributeValues;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsublite.PublisherOptions;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.ProducerRecordCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteStreamingSink extends PTransform<PCollection<EventTransport>, PDone> {

  private static final Logger LOG = LoggerFactory.getLogger(WriteStreamingSink.class);
  private final String outputTopic;
  private final SinkType sinkType;

  private WriteStreamingSink(String outputTopic, SinkType sinkType) {
    this.outputTopic = outputTopic;
    this.sinkType = sinkType;
  }

  public static WriteStreamingSink create(String outputTopic, SinkType sinkType) {
    return new WriteStreamingSink(outputTopic, sinkType);
  }

  @Override
  public void validate(PipelineOptions options) {
    super.validate(options);
    Preconditions.checkArgument(this.outputTopic != null, "An output topic should be provided");
  }

  @Override
  public PDone expand(PCollection<EventTransport> input) {
    switch (this.sinkType) {
      case KAFKA:
        {
          var kafkaConfigs = this.outputTopic.split("/");
          var topic = kafkaConfigs[0];
          var bootstrapServers = kafkaConfigs[1];
          LOG.info("sending data to topic: {} with bootstrap servers {}", topic, bootstrapServers);
          input
              .apply("CreateRecords", ParDo.of(new CreateKafkaRecord(topic)))
              .setCoder(ProducerRecordCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of()))
              .apply(
                  "WriteToKafka",
                  KafkaIO.<byte[], byte[]>writeRecords()
                      .withTopic(topic)
                      .withBootstrapServers(bootstrapServers)
                      .withKeySerializer(ByteArraySerializer.class)
                      .withValueSerializer(ByteArraySerializer.class)
                      .withProducerConfigUpdates(
                          ImmutableMap.of(
                              "max.request.size",
                              "6291456",
                              // needed to connecto to GCP Managed Kafka
                              "security.protocol",
                              "SASL_SSL",
                              "sasl.mechanism",
                              "OAUTHBEARER",
                              "sasl.login.callback.handler.class",
                              "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler",
                              "sasl.jaas.config",
                              "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;")));
          break;
        }
      case PUBSUB:
        {
          input
              .apply(
                  "CreatePubsubMessage",
                  MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                      .via(kvs -> new PubsubMessage(kvs.getData(), kvs.getHeaders(), kvs.getId())))
              .apply("WriteToPubsub", PubsubIO.writeMessages().to(this.outputTopic));
          break;
        }
      case PUBSUBLITE:
        {
          var topicPath = TopicPath.parse(this.outputTopic);
          LOG.info("sending data to topic: {}", topicPath.toString());
          input
              .apply("CreatePubSubLiteMessages", ParDo.of(new CreatePubSubLiteMessage()))
              .apply(
                  "WriteToPubSubLite",
                  PubsubLiteIO.write(
                      PublisherOptions.newBuilder().setTopicPath(topicPath).build()));
          break;
        }
    }
    return PDone.in(input.getPipeline());
  }

  static class CreatePubSubLiteMessage extends DoFn<EventTransport, PubSubMessage> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      var dataPayload = context.element();
      var msgBuilder =
          PubSubMessage.newBuilder()
              .setData(ByteString.copyFrom(dataPayload.getData()))
              .setKey(ByteString.copyFrom(dataPayload.getId().getBytes()))
              .putAllAttributes(
                  dataPayload.getHeaders().entrySet().stream()
                      .map(
                          entry ->
                              KV.of(
                                  entry.getKey(),
                                  AttributeValues.newBuilder()
                                      .addValues(ByteString.copyFrom(entry.getValue().getBytes()))
                                      .build()))
                      .collect(Collectors.toMap(KV::getKey, KV::getValue)));
      // if the element has a ts we used that, if not then we just return the element without that
      // info
      context.output(
          context
              .element()
              .getTransportEpochInMillis()
              .map(
                  ts ->
                      msgBuilder.setEventTime(
                          com.google.protobuf.Timestamp.newBuilder()
                              .setSeconds(ts / 1000L)
                              .setNanos((int) (ts % 1000L) * 1000000)
                              .build()))
              .orElse(msgBuilder)
              .build());
    }
  }

  static class CreateKafkaRecord extends DoFn<EventTransport, ProducerRecord<byte[], byte[]>> {

    private final String topic;

    public CreateKafkaRecord(String topic) {
      this.topic = topic;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      var dataPayload = context.element();
      context.output(
          new ProducerRecord<>(
              this.topic,
              null,
              dataPayload.getId().getBytes(),
              dataPayload.getData(),
              dataPayload.getHeaders().entrySet().stream()
                  .map(entry -> new RecordHeader(entry.getKey(), entry.getValue().getBytes()))
                  .collect(Collectors.toList())));
    }
  }
}
