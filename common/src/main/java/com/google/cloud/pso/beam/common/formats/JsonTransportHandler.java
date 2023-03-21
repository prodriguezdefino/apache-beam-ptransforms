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
package com.google.cloud.pso.beam.common.formats;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.pso.beam.common.transport.Transport;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.everit.json.schema.Schema;
import org.json.JSONObject;

/** A format handler for {@link Transport} objects containing JSON data. */
public record JsonTransportHandler(JsonSchema schema)
    implements TransportFormats.Handler<JSONObject> {

  record JsonSchema(Schema schema) {}

  public JsonTransportHandler(String jsonSchemaLocation) {
    this(new JsonSchema(JsonUtils.retrieveJsonSchemaFromLocation(jsonSchemaLocation)));
  }

  @Override
  public byte[] encode(JSONObject element) {
    return element.toString().getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public JSONObject decode(byte[] encodedElement) {
    try {
      var json = new JSONObject(new String(encodedElement));
      schema.schema().validate(json);
      return json;
    } catch (Exception ex) {
      throw new RuntimeException("Problems while trying to decode element.", ex);
    }
  }

  public TableRow decodeTableRow(byte[] encodedElement) {
    try {
      var bais = new ByteArrayInputStream(encodedElement);
      @SuppressWarnings("deprecation")
      var tableRow = TableRowJsonCoder.of().decode(bais, Coder.Context.OUTER);
      return tableRow;
    } catch (Exception ex) {
      throw new RuntimeException("Problems while trying to decode element.", ex);
    }
  }

  @Override
  public Long longValue(JSONObject element, String propertyName) {
    return TransportFormats.Handler.extractDataWalker(
        element,
        propertyName,
        (jsonObj, propName) -> jsonObj.optJSONObject(propName),
        (jsonObj, propName) -> jsonObj.getLong(propName));
  }

  @Override
  public Double doubleValue(JSONObject element, String propertyName) {
    return TransportFormats.Handler.extractDataWalker(
        element,
        propertyName,
        (jsonObj, propName) -> jsonObj.optJSONObject(propName),
        (jsonObj, propName) -> jsonObj.getDouble(propName));
  }

  @Override
  public String stringValue(JSONObject element, String propertyName) {
    return TransportFormats.Handler.extractDataWalker(
        element,
        propertyName,
        (jsonObj, propName) -> jsonObj.optJSONObject(propName),
        (jsonObj, propName) -> jsonObj.get(propName).toString());
  }
}
