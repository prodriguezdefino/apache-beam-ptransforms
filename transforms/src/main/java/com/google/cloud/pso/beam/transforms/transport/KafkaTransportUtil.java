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
package com.google.cloud.pso.beam.transforms.transport;

import com.google.cloud.pso.beam.common.transport.CommonTransport;
import com.google.cloud.pso.beam.common.transport.EventTransport;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

/**
 * Utility class to resolve Kafka transport creations and handling.
 */
public class KafkaTransportUtil {

  public static SerializableFunction<KafkaRecord<byte[], byte[]>, EventTransport> create() {
    return record -> {
      byte[] data = record.getKV().getValue();
      // check if there is a key, if not then create a random one
      String key = Optional.ofNullable(record.getKV().getKey())
              .map(String::new)
              .orElse(UUID.randomUUID().toString());
      // grab the attributes from the kafka record
      Map<String, String> attrs = StreamSupport
              .stream(record.getHeaders().spliterator(), false)
              .map(header -> KV.of(header.key(), new String(header.value())))
              // in case we have duplicate keys we keep the first one seen
              .collect(Collectors.toMap(KV::getKey, KV::getValue, (at1, at2) -> at1));
      return new CommonTransport(key, attrs, data);
    };
  }
}
