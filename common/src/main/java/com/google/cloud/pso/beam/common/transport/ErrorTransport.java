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

import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/** Represents an error of the processing of a particular event. */
public interface ErrorTransport extends EventTransport {

  static final Schema ERROR_ROW_SCHEMA =
      Schema.builder()
          .addStringField("id")
          .addStringField("errorMessage")
          .addStringField("serializedCause")
          .addStringField("serializedHeaders")
          .addByteArrayField("data")
          .build();

  String getErrorMessage();

  String getSerializedCause();

  default Row toRow() {
    return Row.withSchema(ERROR_ROW_SCHEMA)
        .withFieldValue("id", getId())
        .withFieldValue("errorMessage", getErrorMessage())
        .withFieldValue("serializedCause", getSerializedCause())
        .withFieldValue(
            "serializedHeaders",
            getHeaders().keySet().stream()
                .map(key -> key + "=" + getHeaders().get(key))
                .collect(Collectors.joining(", ", "{", "}")))
        .withFieldValue("data", getData())
        .build();
  }
}
