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
package com.google.cloud.pso.beam.common.formats.options;

import com.google.cloud.pso.beam.common.formats.TransportFormats;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** A collection of options needed to define the configuration for transport formats. */
public interface TransportFormatOptions extends PipelineOptions {

  @Description("The format this pipeline will receive as encoded bytes.")
  @Default.Enum("THRIFT")
  TransportFormats.Format getTransportFormat();

  void setTransportFormat(TransportFormats.Format value);

  @Description("FQCN of the Thrift type the pipeline will ingest. Used when format is THRIFT.")
  @Default.String("com.google.cloud.pso.beam.generator.thrift.CompoundEvent")
  String getThriftClassName();

  void setThriftClassName(String value);

  @Description(
      "The location of the schema file used to understand and deserialize the payload "
          + "from the transport. Used when format is JSON or AVRO.")
  @Default.String("")
  String getSchemaFileLocation();

  void setSchemaFileLocation(String value);
}
