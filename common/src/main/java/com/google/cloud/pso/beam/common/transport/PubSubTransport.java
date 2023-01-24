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
package com.google.cloud.pso.beam.common.transport;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.Instant;

public class PubSubTransport
        implements EventTransport {

  private final PubsubMessage message;

  public PubSubTransport(PubsubMessage message) {
    this.message = message;
  }

  @Override
  public String getId() {
    return this.message.getMessageId();
  }

  @Override
  public Map<String, String> getHeaders() {
    return this.message.getAttributeMap();
  }

  @Override
  public byte[] getData() {
    return this.message.getPayload();
  }

  public PubsubMessage getMessage() {
    return this.message;
  }

  public static SerializableFunction<PubsubMessage, EventTransport> create() {
    return psMessage -> new PubSubTransport(psMessage);
  }

  public static SerializableBiFunction<PubsubMessage, Instant, EventTransport>
          createWithTimestamp() {
    return (psMessage, instant) -> {
      Map<String, String> attributes = Maps.newHashMap(psMessage.getAttributeMap());
      attributes.put("eventTimestamp", instant.toString());
      return new PubSubTransport(
              new PubsubMessage(psMessage.getPayload(), attributes, psMessage.getMessageId()));
    };
  }
}
