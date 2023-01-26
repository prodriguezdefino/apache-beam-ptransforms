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
package com.google.cloud.pso.beam.transforms.transport.coder;

import com.google.cloud.pso.beam.transforms.transport.PubSubTransport;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesAndMessageIdCoder;

public class PubSubTransportCoder
        extends CustomCoder<PubSubTransport> {

  static final PubsubMessageWithAttributesAndMessageIdCoder CODER
          = PubsubMessageWithAttributesAndMessageIdCoder.of();

  public static PubSubTransportCoder of() {
    return new PubSubTransportCoder();
  }

  @Override
  public void encode(PubSubTransport value, OutputStream outStream)
          throws CoderException, IOException {
    CODER.encode(((PubSubTransport) value).getMessage(), outStream);
  }

  @Override
  public PubSubTransport decode(InputStream inStream) throws CoderException, IOException {
    return new PubSubTransport(CODER.decode(inStream));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return Lists.newArrayList();
  }

  @Override
  public void verifyDeterministic() throws Coder.NonDeterministicException {
    CODER.verifyDeterministic();
  }
}
