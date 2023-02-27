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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;

/** A simple coder for the common error transport. */
public class CommonErrorTransportCoder extends CustomCoder<CommonErrorTransport> {

  // The message can be nullable
  private static final Coder<String> MESSAGE_CODER = NullableCoder.of(StringUtf8Coder.of());
  // The serialized cause can also be null
  private static final Coder<String> CAUSE_CODER = NullableCoder.of(StringUtf8Coder.of());
  private static final CommonTransportCoder TRANSPORT_CODER = CommonTransportCoder.of();

  public static Coder<CommonErrorTransport> of(TypeDescriptor<CommonErrorTransport> ignored) {
    return of();
  }

  public static CommonErrorTransportCoder of() {
    return new CommonErrorTransportCoder();
  }

  @Override
  public void encode(CommonErrorTransport value, OutputStream outStream) throws IOException {
    TRANSPORT_CODER.encode(value, outStream);
    MESSAGE_CODER.encode(value.getErrorMessage(), outStream);
    CAUSE_CODER.encode(value.getSerializedCause(), outStream);
  }

  @Override
  public CommonErrorTransport decode(InputStream inStream) throws IOException {
    var commonTransport = TRANSPORT_CODER.decode(inStream);
    var errorMessage = MESSAGE_CODER.decode(inStream);
    var serializedCause = CAUSE_CODER.decode(inStream);
    return CommonErrorTransport.of(commonTransport, errorMessage, serializedCause);
  }
}
