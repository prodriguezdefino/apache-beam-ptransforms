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

import static com.google.cloud.pso.beam.common.formats.TransportFormats.Format.AVRO;
import static com.google.cloud.pso.beam.common.formats.TransportFormats.Format.THRIFT;
import static com.google.cloud.pso.beam.transforms.aggregations.Configuration.AggregationConfigurations;
import static com.google.cloud.pso.beam.transforms.aggregations.Configuration.Aggregations.COUNT;
import static com.google.cloud.pso.beam.transforms.aggregations.Configuration.Aggregations.MAX;
import static com.google.cloud.pso.beam.transforms.aggregations.Configuration.Aggregations.MEAN;
import static com.google.cloud.pso.beam.transforms.aggregations.Configuration.Aggregations.MIN;
import static com.google.cloud.pso.beam.transforms.aggregations.Configuration.Aggregations.SUM;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.cloud.pso.beam.common.Utilities;
import com.google.cloud.pso.beam.common.formats.TransportFormats;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.FileSystems;
import org.joda.time.Duration;

/** */
public class ConfigurationLanguage {

  @FunctionalInterface
  public interface ConfigurationParser {
    AggregationConfigurations apply(InputStream is) throws IOException;
  }

  static AggregationConfigurations readAndParseFromClasspath(
      String location, ConfigurationParser parser) {
    try (var iStream =
        ConfigurationLanguage.class.getResourceAsStream(location.replace("classpath://", "/")); ) {
      return parser.apply(iStream);
    } catch (IOException ex) {
      throw new RuntimeException(
          "Errors while trying to read and parse yaml configuration from classpath: " + location,
          ex);
    }
  }

  static AggregationConfigurations readAndParseFromOtherURL(
      String location, ConfigurationParser parser) {
    try (var iStream =
        Channels.newInputStream(
            FileSystems.open(FileSystems.matchNewResource(location, false))); ) {
      return parser.apply(iStream);
    } catch (IOException ex) {
      throw new RuntimeException(
          "Errors while trying to read and parse yaml configuration from URL: " + location, ex);
    }
  }

  public static AggregationConfigurations parseFromYaml(String location) {
    ConfigurationParser parser =
        (InputStream is) ->
            new ObjectMapper(new YAMLFactory())
                .readValue(is, YamlAggregations.class)
                .toConfigurations();
    if (location.startsWith("classpath://")) {
      return readAndParseFromClasspath(location, parser);
    } else {
      return readAndParseFromOtherURL(location, parser);
    }
  }

  record YamlAggregations(List<YamlAggregation> aggregations) {
    public AggregationConfigurations toConfigurations() {
      return new AggregationConfigurations(
          aggregations.stream()
              .map(
                  aconfig ->
                      (Configuration.AggregationConfiguration)
                          switch (Configuration.Aggregations.valueOf(aconfig.type())) {
                            case COUNT -> new Configuration.Count(
                                aconfig.input().toConfiguration(),
                                aconfig.window().toConfiguration(),
                                aconfig.fields().key());
                            case MAX -> new Configuration.Max(
                                aconfig.input().toConfiguration(),
                                aconfig.window().toConfiguration(),
                                aconfig.fields().key(),
                                aconfig.fields().values());
                            case MIN -> new Configuration.Min(
                                aconfig.input().toConfiguration(),
                                aconfig.window().toConfiguration(),
                                aconfig.fields().key(),
                                aconfig.fields().values());
                            case MEAN -> new Configuration.Mean(
                                aconfig.input().toConfiguration(),
                                aconfig.window().toConfiguration(),
                                aconfig.fields().key(),
                                aconfig.fields().values());
                            case SUM -> new Configuration.Sum(
                                aconfig.input().toConfiguration(),
                                aconfig.window().toConfiguration(),
                                aconfig.fields().key(),
                                aconfig.fields().values());
                          })
              .toList());
    }
  }

  record YamlAggregation(
      String type, YamlWindow window, YamlInputFormat input, YamlAggregationFields fields) {}

  record YamlWindow(String length, String lateness, YamlEarlyFiring earlyFirings) {
    public Configuration.WindowConfiguration toConfiguration() {
      var maybeEarly = Optional.ofNullable(earlyFirings);

      return new Configuration.WindowConfiguration(
          length == null ? Duration.ZERO : Utilities.parseDuration(length),
          lateness == null ? Duration.ZERO : Utilities.parseDuration(lateness),
          maybeEarly.map(early -> early.enabled()).orElse(false),
          maybeEarly.map(early -> early.accumulating()).orElse(false),
          maybeEarly.map(early -> early.count()).orElse(0),
          maybeEarly
              .map(early -> early.time())
              .map(time -> Utilities.parseDuration(time))
              .orElse(Duration.ZERO));
    }
  }

  record YamlEarlyFiring(Boolean enabled, Boolean accumulating, Integer count, String time) {}

  record YamlInputFormat(String format, String thriftClassName, String schemaLocation) {
    public Configuration.InputFormatConfiguration toConfiguration() {
      return switch (TransportFormats.Format.valueOf(format)) {
        case AVRO -> new Configuration.AvroFormat(schemaLocation);
        case THRIFT -> new Configuration.ThriftFormat(thriftClassName);
        case AGGREGATION_RESULT -> new Configuration.AggregationResultFormat();
        case JSON -> new Configuration.JSONFormat(schemaLocation);
      };
    }
  }

  record YamlAggregationFields(List<String> key, List<String> values) {}
}
