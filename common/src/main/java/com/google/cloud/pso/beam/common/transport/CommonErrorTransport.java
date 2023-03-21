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

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/** Simple implementation for an ErrorTransport */
public record CommonErrorTransport(
    String id,
    byte[] erroredData,
    Map<String, String> headers,
    String serializedCause,
    String errorMessage)
    implements ErrorTransport {

  public CommonErrorTransport(
      String id, Map<String, String> headers, byte[] data, Throwable cause, String errorMessage) {
    this(
        id,
        data,
        headers,
        cause.getMessage()
            + "\n"
            + Arrays.stream(cause.getStackTrace())
                .map(se -> se.toString())
                .collect(Collectors.joining("\n")),
        errorMessage);
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public Map<String, String> getHeaders() {
    return headers;
  }

  @Override
  public byte[] getErroredData() {
    return erroredData;
  }

  @Override
  public String getSerializedCause() {
    return serializedCause;
  }

  @Override
  public String getErrorMessage() {
    return errorMessage;
  }

  public static CommonErrorTransport of(EventTransport transport, String message, Throwable cause) {
    return new CommonErrorTransport(
        transport.getId(), transport.getHeaders(), transport.getData(), cause, message);
  }

  public static CommonErrorTransport of(EventTransport transport, String message, String cause) {
    return new CommonErrorTransport(
        transport.getId(), transport.getData(), transport.getHeaders(), cause, message);
  }

  public static CommonErrorTransport of(
      String id,
      byte[] data,
      Map<String, String> headers,
      String serializedCause,
      String errorMessage) {
    return new CommonErrorTransport(id, data, headers, serializedCause, errorMessage);
  }

  public static CommonErrorTransport of(Transport transport, String message, Throwable cause) {
    return new CommonErrorTransport(
        transport.getId(), transport.getHeaders(), new byte[0], cause, message);
  }
}
