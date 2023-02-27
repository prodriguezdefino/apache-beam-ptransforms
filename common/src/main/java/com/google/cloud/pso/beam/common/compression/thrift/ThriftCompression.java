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
package com.google.cloud.pso.beam.common.compression.thrift;

import com.google.cloud.pso.beam.envelope.Element;
import com.google.cloud.pso.beam.envelope.Envelope;
import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TZlibTransport;

/** Utility functions for compression and decompression of thrift based data. */
public class ThriftCompression {

  public static Element constructElement(byte[] data, Map<String, String> headers) {
    var element = new Element();
    element.setHeaders(headers);
    element.setData(ByteBuffer.wrap(data));
    return element;
  }

  public static Envelope constructEnvelope(List<Element> element, Map<String, String> headers) {
    var envelope = new Envelope();
    envelope.setHeaders(headers);
    envelope.setElements(element);
    return envelope;
  }

  @VisibleForTesting
  public static byte[] compressEnvelope(Envelope envelope, int compressionLevel) throws TException {
    var baos = new ByteArrayOutputStream();
    var transport = new TZlibTransport(new TIOStreamTransport(baos), compressionLevel);
    var protocol = new TBinaryProtocol.Factory().getProtocol(transport);
    baos.reset();
    envelope.write(protocol);
    transport.flush();
    return baos.toByteArray();
  }

  @VisibleForTesting
  public static Envelope decompressEnvelope(byte[] data) throws TException {
    var tMemoryInputTransport = new TMemoryInputTransport();
    var transport = new TZlibTransport(tMemoryInputTransport);
    var protocol = new TBinaryProtocol.Factory().getProtocol(transport);
    tMemoryInputTransport.reset(data, 0, data.length);
    var records = new Envelope();
    records.read(protocol);
    tMemoryInputTransport.clear();
    protocol.reset();
    return records;
  }
}
