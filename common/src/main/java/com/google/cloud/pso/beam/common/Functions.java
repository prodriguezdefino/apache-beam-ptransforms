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
package com.google.cloud.pso.beam.common;

import java.io.Serializable;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * A compilation of interfaces that enable function references to be used in Beam related contexts.
 */
public class Functions {

  @FunctionalInterface
  public interface SerializableProvider<OutputT> extends Serializable {

    /**
     * Returns the result of invoking this function on the given input.
     *
     * @return
     */
    OutputT apply();
  }

  /**
   * Enables the partial execution of a {@link SerializableBiFunction} in multiple steps.
   *
   * @param <A> Type of function's first parameter.
   * @param <B> Type of function's second parameter.
   * @param <R> Type of function's return.
   * @param biFunc SerializableBiFunction instance to partially apply.
   * @return a curried function representation.
   */
  public static <A, B, R> SerializableFunction<A, SerializableFunction<B, R>> curry(
      SerializableBiFunction<A, B, R> biFunc) {
    return (a) -> (b) -> biFunc.apply(a, b);
  }

  /**
   * Intended to model the function the Compose transform will use to read compose part files and
   * write them into the compose.
   *
   * @param <SinkT>
   */
  @FunctionalInterface
  public interface ComposeFunction<SinkT extends FileIO.Sink> extends Serializable {

    /**
     * Returns the result of invoking this function given the output.
     *
     * @param sink A Beam sink implementation for the determined type
     * @param composeDestination the location for the compose file
     * @param composePartLocations an iterable with the location of all the compose part files
     * @return True in case the compose file was successfully written, false otherwise.
     */
    Boolean apply(
        SinkT sink, String composeDestination, Iterable<FileIO.ReadableFile> composePartLocations);
  }
}
