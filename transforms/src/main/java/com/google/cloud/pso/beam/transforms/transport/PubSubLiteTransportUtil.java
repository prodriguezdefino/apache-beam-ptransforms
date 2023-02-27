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
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;

/** Utility class to resolve Kafka transport creations and handling. */
public class PubSubLiteTransportUtil {

  public static SerializableFunction<SequencedMessage, CommonTransport> create() {
    return sqMsg -> {
      PubSubMessage msg = sqMsg.getMessage();
      byte[] data = msg.getData().toByteArray();
      Map<String, String> attrs =
          msg.getAttributesMap().entrySet().stream()
              .map(
                  entry ->
                      KV.of(
                          entry.getKey(),
                          new String(entry.getValue().getValuesList().get(0).toByteArray())))
              .collect(Collectors.toMap(KV::getKey, KV::getValue));
      return new CommonTransport(msg.getKey().toStringUtf8(), attrs, data);
    };
  }
}
