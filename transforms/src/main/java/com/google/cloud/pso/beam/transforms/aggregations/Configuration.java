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
package com.google.cloud.pso.beam.transforms.aggregations;

import com.google.cloud.pso.beam.common.formats.TransportFormats.Format;
import com.google.cloud.pso.beam.options.AggregationOptions;
import java.io.Serializable;
import java.util.List;
import org.joda.time.Duration;

/** */
public class Configuration {
  public enum Aggregations {
    COUNT,
    MIN,
    MAX,
    MEAN,
    SUM;
  }

  public record AggregationConfigurations(List<AggregationConfiguration> configurations)
      implements Serializable {

    public static AggregationConfigurations fromOptions(AggregationOptions options) {
      return ConfigurationLanguage.parseFromYaml(options.getAggregationConfigurationLocation());
    }
  }

  public sealed interface AggregationConfiguration permits Count, Sum, Min, Max, Mean {
    String name();

    InputFormatConfiguration format();

    WindowConfiguration window();

    List<String> keyFields();

    List<String> valueFields();
  }

  public sealed interface InputFormatConfiguration
      permits ThriftFormat, AvroFormat, AggregationResultFormat, JSONFormat {
    Format format();
  }

  public record ThriftFormat(String className) implements InputFormatConfiguration, Serializable {

    @Override
    public Format format() {
      return Format.THRIFT;
    }
  }

  public record JSONFormat(String schemaLocation)
      implements InputFormatConfiguration, Serializable {

    @Override
    public Format format() {
      return Format.JSON;
    }
  }

  public record AvroFormat(String schemaLocation)
      implements InputFormatConfiguration, Serializable {

    @Override
    public Format format() {
      return Format.AVRO;
    }
  }

  public record AggregationResultFormat() implements InputFormatConfiguration, Serializable {

    @Override
    public Format format() {
      return Format.AGGREGATION_RESULT;
    }
  }

  public record WindowConfiguration(
      Duration length,
      Duration lateness,
      Boolean withEarlyFirings,
      Boolean shouldAccumulatePanes,
      Integer earlyFireCount,
      Duration earlyFiringTime)
      implements Serializable {}

  public record Count(
      InputFormatConfiguration format, WindowConfiguration window, List<String> keyFields)
      implements AggregationConfiguration, Serializable {

    @Override
    public List<String> valueFields() {
      return List.of();
    }

    @Override
    public String name() {
      return Aggregations.COUNT.name().toLowerCase();
    }
  }

  public record Sum(
      InputFormatConfiguration format,
      WindowConfiguration window,
      List<String> keyFields,
      List<String> valueFields)
      implements AggregationConfiguration, Serializable {

    @Override
    public String name() {
      return Aggregations.SUM.name().toLowerCase();
    }
  }

  public record Min(
      InputFormatConfiguration format,
      WindowConfiguration window,
      List<String> keyFields,
      List<String> valueFields)
      implements AggregationConfiguration, Serializable {

    @Override
    public String name() {
      return Aggregations.MIN.name().toLowerCase();
    }
  }

  public record Max(
      InputFormatConfiguration format,
      WindowConfiguration window,
      List<String> keyFields,
      List<String> valueFields)
      implements AggregationConfiguration, Serializable {

    @Override
    public String name() {
      return Aggregations.MAX.name().toLowerCase();
    }
  }

  public record Mean(
      InputFormatConfiguration format,
      WindowConfiguration window,
      List<String> keyFields,
      List<String> valueFields)
      implements AggregationConfiguration, Serializable {

    @Override
    public String name() {
      return Aggregations.MEAN.name().toLowerCase();
    }
  }
}
