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
package com.google.cloud.pso.beam.common.transport;

import java.util.Map;
import java.util.Optional;
import org.joda.time.Instant;

/**
 * A transport interface that should cover the needed methods to move data between sources and
 * sinks.
 */
public interface EventTransport {

  static final String EVENT_TIME_KEY = "eventTimestamp";
  static final String SCHEMA_ATTRIBUTE_KEY = "SCHEMA_ATTRIBUTE";

  String getId();

  Map<String, String> getHeaders();

  byte[] getData();

  /**
   * In case the headers of this event transport contains a key named "eventTimestamp" with a value
   * representing a ISO timestamp, this method will return an Optional containing the millis from
   * epoch represented in that ISO timestamp; if the attribute key is not present the Optional is
   * empty.
   *
   * @return Optional with potentially a value of millis since epoch
   */
  default Optional<Long> getEventEpochInMillis() {
    return Optional.ofNullable(this.getHeaders().get(EVENT_TIME_KEY))
        .map(EventTransport::fromStringTimestamp)
        .orElse(Optional.empty());
  }

  static Optional<Long> fromStringTimestamp(String stringTimestamp) {
    try {
      return Optional.of(Instant.parse(stringTimestamp).getMillis());
    } catch (Exception ex) {
      return Optional.empty();
    }
  }
}
