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

import com.google.cloud.pso.beam.common.transport.CommonTransport;
import com.google.cloud.pso.beam.common.transport.EventTransport;
import java.time.Instant;
import java.util.HashMap;

/** A custom UDF that decides on a positive sentiment based on the page score. */
public class CustomUDF implements UDF {

  @Override
  public EventTransport apply(EventTransport event) throws RuntimeException {

    var newAttributes = new HashMap<String, String>(event.getHeaders());
    // we can add the execution time
    newAttributes.put("udfExecTimestamp", Instant.now().toString());

    return new CommonTransport(event.getId(), newAttributes, event.getData());
  }
}
