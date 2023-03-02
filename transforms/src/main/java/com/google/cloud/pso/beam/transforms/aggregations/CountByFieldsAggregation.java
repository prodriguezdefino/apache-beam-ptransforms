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

import com.google.cloud.pso.beam.common.formats.options.TransportFormatOptions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a count aggregation which groups data based on a set of configured fields. Also
 * enables the configuration of the window's length and trigger frequencies.
 */
public class CountByFieldsAggregation extends BaseAggregation<String, Long, Long> {

  private static final Logger LOG = LoggerFactory.getLogger(CountByFieldsAggregation.class);

  CountByFieldsAggregation() {}

  public static CountByFieldsAggregation create() {
    return new CountByFieldsAggregation();
  }

  @Override
  protected Coder<String> keyCoder() {
    return StringUtf8Coder.of();
  }

  @Override
  protected Coder<Long> resultCoder() {
    return VarLongCoder.of();
  }

  @SuppressWarnings("unchecked")
  @Override
  protected SerializableBiFunction<List<String>, Object, String> keyExtractorFunction(
      TransportFormatConfiguration config) {
    var handler = FORMAT_HANDLER_FUNC.apply(config);

    return (keyFieldList, decodedData) ->
        keyFieldList.stream()
            .map(keyField -> keyField + "#" + handler.stringValue(decodedData, keyField))
            .collect(Collectors.joining("#"));
  }

  @Override
  protected SerializableBiFunction<List<String>, Object, Map<String, Long>> valuesExtractorFunction(
      TransportFormatConfiguration config) {
    return (ignoredFieldList, ignoredDecodeObject) -> Map.of("count", 1L);
  }

  @Override
  protected MapElements<BaseAggregationTransport<String, Long>, List<KV<String, Long>>>
      transportMapper() {
    return MapElements.into(
            TypeDescriptors.lists(
                TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs())))
        .via(
            tr ->
                tr.getMappedValues().entrySet().stream()
                    .map(
                        entry ->
                            KV.of(tr.getAggregationKey() + "#" + entry.getKey(), entry.getValue()))
                    .toList());
  }

  @Override
  protected PTransform<PCollection<KV<String, Long>>, PCollection<KV<String, Long>>> aggregation() {
    return Count.<String, Long>perKey();
  }
}
