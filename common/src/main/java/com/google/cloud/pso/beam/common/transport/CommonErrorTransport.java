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
public class CommonErrorTransport extends CommonTransport implements ErrorTransport {

  private final String serializedCause;
  private final String errorMessage;

  public CommonErrorTransport(
      String id, Map<String, String> headers, byte[] data, Throwable cause, String errorMessage) {
    super(id, headers, data);
    this.serializedCause =
        cause.getMessage()
            + "\n"
            + Arrays.stream(cause.getStackTrace())
                .map(se -> se.toString())
                .collect(Collectors.joining("\n"));
    this.errorMessage = errorMessage;
  }

  public CommonErrorTransport(
      String id,
      Map<String, String> headers,
      byte[] data,
      String serializedCause,
      String errorMessage) {
    super(id, headers, data);
    this.serializedCause = serializedCause;
    this.errorMessage = errorMessage;
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
        transport.getId(), transport.getHeaders(), transport.getData(), cause, message);
  }
}
