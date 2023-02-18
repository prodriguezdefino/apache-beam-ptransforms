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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;
import com.google.cloud.pso.beam.options.CountByFieldsAggregationOptions;

/**
 *
 */
public class CountAggregationTest {

  @Test
  public void testOptions() throws JsonProcessingException {
    String[] args = {
      "--aggregationKeyNames=uuid"
    };

    var options = PipelineOptionsFactory
            .fromArgs(args).withValidation().as(CountByFieldsAggregationOptions.class);
    var countConfig = options.getCountConfiguration();
    var json = new ObjectMapper().writeValueAsString(countConfig);

    Assert.assertNotNull(json);
    Assert.assertEquals(1, countConfig.getKeyFields().size());
  }

}
