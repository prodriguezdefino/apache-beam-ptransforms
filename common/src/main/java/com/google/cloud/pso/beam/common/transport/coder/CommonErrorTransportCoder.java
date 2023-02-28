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
package com.google.cloud.pso.beam.common.transport.coder;

import com.google.cloud.pso.beam.common.transport.CommonErrorTransport;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A simple coder for the common error transport. */
public class CommonErrorTransportCoder extends CustomCoder<CommonErrorTransport> {

  // The message can be nullable
  private static final Coder<String> MESSAGE_CODER = NullableCoder.of(StringUtf8Coder.of());
  // The serialized cause can also be null
  private static final Coder<String> CAUSE_CODER = NullableCoder.of(StringUtf8Coder.of());
  // A message's payload cannot be null
  private static final Coder<byte[]> DATA_CODER = ByteArrayCoder.of();
  // A message's attributes can be null.
  private static final Coder<Map<String, String>> HEADERS_CODER =
      NullableCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
  // A message's messageId may be null at some moments in the execution
  private static final Coder<String> ID_CODER = NullableCoder.of(StringUtf8Coder.of());

  public static Coder<CommonErrorTransport> of(TypeDescriptor<CommonErrorTransport> ignored) {
    return of();
  }

  public static CommonErrorTransportCoder of() {
    return new CommonErrorTransportCoder();
  }

  @Override
  public void encode(CommonErrorTransport value, OutputStream outStream) throws IOException {
    ID_CODER.encode(value.getId(), outStream);
    DATA_CODER.encode(value.getErroredData(), outStream);
    HEADERS_CODER.encode(value.getHeaders(), outStream);
    MESSAGE_CODER.encode(value.getErrorMessage(), outStream);
    CAUSE_CODER.encode(value.getSerializedCause(), outStream);
  }

  @Override
  public CommonErrorTransport decode(InputStream inStream) throws IOException {
    var id = ID_CODER.decode(inStream);
    var data = DATA_CODER.decode(inStream);
    var headers = HEADERS_CODER.decode(inStream);
    var errorMessage = MESSAGE_CODER.decode(inStream);
    var serializedCause = CAUSE_CODER.decode(inStream);
    return CommonErrorTransport.of(id, data, headers, errorMessage, serializedCause);
  }
}
