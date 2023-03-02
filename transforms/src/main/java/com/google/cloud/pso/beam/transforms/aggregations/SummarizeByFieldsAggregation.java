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

import static com.google.cloud.pso.beam.transforms.aggregations.BaseAggregation.FORMAT_HANDLER_FUNC;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Mean;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Represents a base class for all aggregations that will summarize data. The specific aggregations
 * can be instantiated by using the static methods existing in this class.
 */
public abstract class SummarizeByFieldsAggregation extends BaseAggregation<String, Double, Double> {

  /**
   * Returns a MIN by field aggregation PTransform.
   *
   * @return the MIN aggregation
   */
  public static MinByFieldAggregation min() {
    return new MinByFieldAggregation();
  }

  /**
   * Returns a MAX by field aggregation PTransform.
   *
   * @return the MAX aggregation
   */
  public static MaxByFieldAggregation max() {
    return new MaxByFieldAggregation();
  }
  /**
   * Returns a MEAN by field aggregation PTransform.
   *
   * @return the MEAN aggregation
   */
  public static MeanByFieldAggregation mean() {
    return new MeanByFieldAggregation();
  }

  /**
   * Returns a SUM by field aggregation PTransform.
   *
   * @return the SUM aggregation
   */
  public static SumByFieldAggregation sum() {
    return new SumByFieldAggregation();
  }

  @Override
  protected Coder<String> keyCoder() {
    return StringUtf8Coder.of();
  }

  @Override
  protected Coder<Double> resultCoder() {
    return DoubleCoder.of();
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

  @SuppressWarnings("unchecked")
  @Override
  protected SerializableBiFunction<List<String>, Object, Map<String, Double>>
      valuesExtractorFunction(TransportFormatConfiguration config) {
    var handler = FORMAT_HANDLER_FUNC.apply(config);
    return (valueFieldList, decodedData) ->
        valueFieldList.stream()
            .map(valueField -> KV.of(valueField, handler.doubleValue(decodedData, valueField)))
            .collect(Collectors.toMap(KV::getKey, KV::getValue));
  }

  @Override
  protected MapElements<BaseAggregationTransport<String, Double>, List<KV<String, Double>>>
      transportMapper() {
    return MapElements.into(
            TypeDescriptors.lists(
                TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.doubles())))
        .via(
            tr ->
                tr.getMappedValues().entrySet().stream()
                    .map(
                        entry ->
                            KV.of(tr.getAggregationKey() + "#" + entry.getKey(), entry.getValue()))
                    .toList());
  }

  /**
   * This aggregation calculates the MIN value existing on the provided {@link Transport} inputs and
   * returns the smallest value present given the window configuration.
   */
  public static class MinByFieldAggregation extends SummarizeByFieldsAggregation {

    MinByFieldAggregation() {}

    @Override
    protected PTransform<PCollection<KV<String, Double>>, PCollection<KV<String, Double>>>
        aggregation() {
      return Min.<String>doublesPerKey();
    }
  }
  /**
   * This aggregation calculates the MAX value existing on the provided {@link Transport} inputs and
   * returns the biggest value present given the window configuration.
   */
  public static class MaxByFieldAggregation extends SummarizeByFieldsAggregation {

    MaxByFieldAggregation() {}

    @Override
    protected PTransform<PCollection<KV<String, Double>>, PCollection<KV<String, Double>>>
        aggregation() {
      return Max.<String>doublesPerKey();
    }
  }

  /**
   * This aggregation calculates the MIN value existing on the provided {@link Transport} inputs and
   * returns the addition of all the value present given the window configuration.
   */
  public static class SumByFieldAggregation extends SummarizeByFieldsAggregation {

    SumByFieldAggregation() {}

    @Override
    protected PTransform<PCollection<KV<String, Double>>, PCollection<KV<String, Double>>>
        aggregation() {
      return Sum.<String>doublesPerKey();
    }
  }

  /**
   * This aggregation calculates the Mean value existing on the provided {@link Transport} inputs
   * and returns the arithmetic mean of all the value present given the window configuration.
   */
  public static class MeanByFieldAggregation extends SummarizeByFieldsAggregation {

    MeanByFieldAggregation() {}

    @Override
    protected PTransform<PCollection<KV<String, Double>>, PCollection<KV<String, Double>>>
        aggregation() {
      return Mean.<String, Double>perKey();
    }
  }
}
