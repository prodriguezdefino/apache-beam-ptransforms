package com.google.cloud.pso.beam.common.compression.thrift;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import com.google.common.annotations.VisibleForTesting;
import com.google.cloud.pso.beam.envelope.Element;
import com.google.cloud.pso.beam.envelope.Envelope;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TZlibTransport;

/**
 * Utility functions for compression and decompression of thrift based data.
 */
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
