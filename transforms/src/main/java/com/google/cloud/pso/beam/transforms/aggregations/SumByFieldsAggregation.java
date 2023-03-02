/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

package com.google.cloud.pso.beam.transforms.aggregations;

import com.google.cloud.pso.beam.common.formats.options.TransportFormatOptions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

/** */
public class SumByFieldsAggregation extends BaseAggregation<String, Double, Double> {
  SumByFieldsAggregation() {}

  public static SumByFieldsAggregation create() {
    return new SumByFieldsAggregation();
  }

  @Override
  protected Coder<String> keyCoder() {
    return StringUtf8Coder.of();
  }

  @Override
  protected Coder<Double> resultCoder() {
    return DoubleCoder.of();
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

  @Override
  protected PTransform<PCollection<KV<String, Double>>, PCollection<KV<String, Double>>>
      aggregation() {
    return Sum.<String>doublesPerKey();
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
}
