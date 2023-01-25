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
package com.google.cloud.pso.beam.udf.transforms;

import com.google.cloud.pso.beam.common.transport.EventTransport;
import com.google.cloud.pso.beam.udf.UDF;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enables the execution of user defined functions that can transform a single element at a time.
 */
public class ExecuteUDF
        extends PTransform<PCollection<EventTransport>, PCollection<EventTransport>> {

  private static final Logger LOG = LoggerFactory.getLogger(ExecuteUDF.class);

  private final String udfClassName;

  ExecuteUDF(String className) {
    this.udfClassName = className;
  }

  public static ExecuteUDF create(String udfClassName) {
    return new ExecuteUDF(udfClassName);
  }

  @Override
  public PCollection<EventTransport> expand(PCollection<EventTransport> input) {
    return input.apply("ExecuteUDF", ParDo.of(new ExecuteUDFDoFn(udfClassName)));
  }

  /**
   * In charge of loading the configured UDF and execute it on every processed row.
   */
  static class ExecuteUDFDoFn extends DoFn<EventTransport, EventTransport> {

    private UDF udf;
    private final String className;

    public ExecuteUDFDoFn(String className) {
      this.className = className;
    }

    @Setup
    public void setup() {
      this.udf = Optional
              .ofNullable(this.className)
              .filter(cName -> !cName.isBlank())
              .map(ExecuteUDFDoFn::loadUDF)
              .orElseGet(DefaultUDF::new);
    }

    @ProcessElement
    public void process(ProcessContext context) {
      context.output(udf.apply(context.element()));
    }

    /**
     * Default implementation class if no UDF is provided.
     */
    static class DefaultUDF implements UDF {
    }

    static UDF loadUDF(String className) {
      try {
        var clazz
                = Class.forName(className, true, ExecuteUDFDoFn.class.getClassLoader());
        return (UDF) clazz.getDeclaredConstructor().newInstance();
      } catch (ClassNotFoundException
              | IllegalAccessException | IllegalArgumentException
              | InstantiationException | NoSuchMethodException
              | SecurityException | InvocationTargetException ex) {
        LOG.error(
                "Problems while loading the requested UDF class name: " + className, ex);
        return null;
      }
    }
  }
}
