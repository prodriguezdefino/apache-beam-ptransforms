/*
 * Copyright (C) 2022 Google Inc.
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
package com.google.cloud.pso.beam.common.formats.options;

import com.google.cloud.pso.beam.common.formats.TransportFormats;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * A collection of options needed to define the configuration for transport formats.
 */
public interface TransportFormatOptions extends PipelineOptions {

  @Description("FQCN of the type the pipeline will ingest, sets EventFormat as THRIFT.")
  @Default.String("com.google.cloud.pso.beam.generator.thrift.CompoundEvent")
  String getThriftClassName();

  void setThriftClassName(String value);

  @Description("The Avro schema file used to deserialize the payload from the event, "
          + "sets EventFormat as AVRO. Takes precedence over other settings in this options.")
  @Default.String("")
  String getAvroSchemaLocation();

  void setAvroSchemaLocation(String value);

  @Description("The identified EventFormat for the pipeline's data. Tipically "
          + "this would be automatically configured based on the other options set at launch.")
  @Default.InstanceFactory(TransportFormatFactory.class)
  TransportFormats.Format getTransportFormat();

  void setTransportFormat(TransportFormats.Format value);

  static class TransportFormatFactory
          implements DefaultValueFactory<TransportFormats.Format> {

    @Override
    public TransportFormats.Format create(PipelineOptions options) {
      var eventOptions = options.as(TransportFormatOptions.class);
      if (eventOptions.getAvroSchemaLocation().isBlank()
              || eventOptions.getAvroSchemaLocation().isEmpty()) {
        return TransportFormats.Format.THRIFT;
      } else {
        return TransportFormats.Format.AVRO;
      }
    }
  }
}
