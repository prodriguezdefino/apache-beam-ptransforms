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

import com.google.cloud.pso.beam.common.transport.coder.CommonTransportCoder;
import java.util.Map;
import org.apache.beam.sdk.coders.DefaultCoder;

/** Immutable generic transport. */
@DefaultCoder(CommonTransportCoder.class)
public class CommonTransport implements EventTransport {

  private final String id;
  private final Map<String, String> headers;
  private final byte[] data;

  public CommonTransport(String id, Map<String, String> headers, byte[] data) {
    this.id = id;
    this.headers = headers;
    this.data = data;
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
  public byte[] getData() {
    return data;
  }

  public static CommonTransport of(EventTransport transport) {
    return new CommonTransport(transport.getId(), transport.getHeaders(), transport.getData());
  }
}
