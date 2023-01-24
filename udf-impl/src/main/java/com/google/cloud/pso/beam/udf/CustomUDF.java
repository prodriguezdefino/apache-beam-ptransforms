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

import java.util.Optional;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;

/**
 * A custom UDF that decides on a positive sentiment based on the page score. 
 */
public class CustomUDF implements UDF {

  @Override
  public Row apply(Row row) throws RuntimeException {
    
    return Row.fromRow(row)
            .withFieldValue("sentiment", 
                    Optional.of(row.getInt32("page_score"))
                            .map(score -> score > 5 ? "positive" : "negative").get())
            .withFieldValue("processing_time", DateTime.now())
            .build();
  }

}
