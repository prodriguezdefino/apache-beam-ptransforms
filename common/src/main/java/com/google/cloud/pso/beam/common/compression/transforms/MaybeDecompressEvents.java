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
package com.google.cloud.pso.beam.common.compression.transforms;

import com.google.cloud.pso.beam.common.compression.CompressionUtils;
import com.google.cloud.pso.beam.common.compression.thrift.ThriftCompression;
import com.google.cloud.pso.beam.common.transport.CommonErrorTransport;
import com.google.cloud.pso.beam.common.transport.CommonTransport;
import com.google.cloud.pso.beam.common.transport.ErrorTransport;
import com.google.cloud.pso.beam.common.transport.EventTransport;
import com.google.cloud.pso.beam.common.transport.coder.CommonErrorTransportCoder;
import com.google.cloud.pso.beam.common.transport.coder.CommonTransportCoder;
import com.google.cloud.pso.beam.envelope.Envelope;
import com.google.common.collect.Lists;
import java.util.Optional;
import java.util.UUID;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This transform will review the headers potentially present in the transport object and decide if
 * it needs to decompress and potentially break the batch of the contents depending on the
 * configured compression method.
 */
public class MaybeDecompressEvents
        extends PTransform<PCollection<? extends EventTransport>, PCollectionTuple> {

  public static TupleTag<CommonTransport> SUCCESSFULLY_PROCESSED_EVENTS = new TupleTag<>() {
  };
  public static TupleTag<ErrorTransport> FAILED_EVENTS = new TupleTag<>() {
  };

  private static final Logger LOG = LoggerFactory.getLogger(MaybeDecompressEvents.class);

  MaybeDecompressEvents() {
  }

  public static MaybeDecompressEvents create() {
    return new MaybeDecompressEvents();
  }

  @Override
  public PCollectionTuple expand(PCollection<? extends EventTransport> input) {
    input.getPipeline().getCoderRegistry().registerCoderForClass(
            CommonTransport.class, CommonTransportCoder.of());
    input.getPipeline().getCoderRegistry().registerCoderForClass(
            CommonErrorTransport.class, CommonErrorTransportCoder.of());
    return input.apply("CheckHeadersAndDecompressIfPresent",
            ParDo.of(new CheckForDecompression())
                    .withOutputTags(SUCCESSFULLY_PROCESSED_EVENTS, TupleTagList.of(FAILED_EVENTS)));
  }

  static class CheckForDecompression extends DoFn<EventTransport, CommonTransport> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      try {
        if (!CompressionUtils.CompressionType.shouldDecompress(
                context.element().getHeaders().get(
                        CompressionUtils.COMPRESSION_TYPE_HEADER_KEY))) {
          context.output(SUCCESSFULLY_PROCESSED_EVENTS, CommonTransport.of(context.element()));
          return;
        }
        switch (CompressionUtils.CompressionType.valueOf(
                context.element().getHeaders().get(
                        CompressionUtils.COMPRESSION_TYPE_HEADER_KEY))) {
          case AVRO_SNAPPY:
            throw new RuntimeException("Avro compression is not implemented yet");
          case THRIFT_ZLIB: {
            try {
              var envelope = Optional.ofNullable(
                      ThriftCompression.decompressEnvelope(context.element().getData()));
              envelope
                      .map(Envelope::getElements)
                      .orElse(Lists.newArrayList())
                      .forEach(
                              element
                              -> context.output(
                                      SUCCESSFULLY_PROCESSED_EVENTS,
                                      new CommonTransport(
                                              UUID.randomUUID().toString(),
                                              element.getHeaders(),
                                              element.getData())));
            } catch (TException ex) {
              LOG.error("Can't decompress payload, bailing for now.", ex);
            }
            break;
          }
          default: {
            LOG.warn("we shouldn't have arrived here, lets continue the pipeline :shrugs:");
            context.output(SUCCESSFULLY_PROCESSED_EVENTS, CommonTransport.of(context.element()));
          }
        }
      } catch (Exception ex) {
        var msg = "Errors occurred while trying to decompress the transport payload.";
        LOG.error(msg, ex);
        context.output(FAILED_EVENTS, CommonErrorTransport.of(context.element(), msg, ex));
      }
    }
  }
}
