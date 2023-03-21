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

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.cloud.pso.beam.common.formats.InputFormatConfiguration;
import com.google.cloud.pso.beam.transforms.aggregations.Configuration.AggregationConfigurations;
import com.google.cloud.pso.beam.transforms.aggregations.Configuration.Count;
import com.google.cloud.pso.beam.transforms.aggregations.Configuration.Sum;
import java.io.IOException;
import org.junit.Test;

/** */
public class ConfigLanguageTest {
  @Test
  public void testYamlToConfigurations() throws IOException {
    var mapper = new ObjectMapper(new YAMLFactory());
    var yamlConfig =
        mapper.readValue(
            this.getClass()
                .getClassLoader()
                .getResourceAsStream("aggregations/aggregations-config.yml"),
            ConfigurationLanguage.YamlAggregations.class);
    assertNotNull(yamlConfig);
    assertEquals(2, yamlConfig.aggregations().size());
    var yamlAgg = yamlConfig.aggregations().get(0);
    assertEquals("COUNT", yamlAgg.type());
    assertEquals(1, yamlAgg.fields().key().size());
    assertEquals(0, yamlAgg.fields().values().size());
    var aggregations = yamlConfig.toConfigurations();
    validateConfig(aggregations);
  }

  @Test
  public void testConfigurationParser() throws IOException {
    var aggregations =
        ConfigurationLanguage.parseFromYaml("classpath://aggregations/aggregations-config.yml");
    validateConfig(aggregations);
  }

  private void validateConfig(AggregationConfigurations aggregations) {
    assertNotNull(aggregations);
    assertEquals(2, aggregations.configurations().size());
    assertTrue(
        aggregations.configurations().get(0) instanceof Count count
            && count.format() instanceof InputFormatConfiguration.ThriftFormat
            && count.window().shouldAccumulatePanes()
            && count.window().withEarlyFirings()
            && count.valueFields().isEmpty());
    assertTrue(
        aggregations.configurations().get(1) instanceof Sum sum
            && sum.format() instanceof InputFormatConfiguration.AvroFormat
            && !sum.window().withEarlyFirings()
            && !sum.valueFields().isEmpty());
  }
}
