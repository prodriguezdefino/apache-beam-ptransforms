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
package com.google.cloud.pso.beam.udf;

import com.google.cloud.pso.beam.common.transport.EventTransport;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Represents a custom function that can be implemented to transform a Beam Row object. */
public interface UDF extends SerializableFunction<EventTransport, EventTransport> {

  /**
   * Function that applies the transformation to the input object. By default this method returns
   * the input object as-is.
   *
   * @param event The input event
   * @return the transformed event
   * @throws RuntimeException If errors occur while executing the transformation.
   */
  @Override
  default EventTransport apply(EventTransport event) throws RuntimeException {
    return event;
  }
}
