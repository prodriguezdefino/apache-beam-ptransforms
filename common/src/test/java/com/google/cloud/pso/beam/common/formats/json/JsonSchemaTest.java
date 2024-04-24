package com.google.cloud.pso.beam.common.formats.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.saasquatch.jsonschemainferrer.AdditionalPropertiesPolicies;
import com.saasquatch.jsonschemainferrer.JsonSchemaInferrer;
import com.saasquatch.jsonschemainferrer.RequiredPolicies;
import com.saasquatch.jsonschemainferrer.SpecVersion;
import org.junit.Test;

/** */
public class JsonSchemaTest {

  private static final String JSON_OBJECT =
      "{\n"
          + "  \"name\": \"John Doe\",  \n"
          + "  \"age\": 30,\n"
          + "  \"isEmployed\": true, \n"
          + "  \"skills\": [\"Programming\", \"Web Development\", \"Data Analysis\"], \n"
          + "  \"address\": {        \n"
          + "    \"street\": \"123 Main St\",\n"
          + "    \"city\": \"Anytown\",\n"
          + "    \"state\": \"CA\",\n"
          + "    \"zipCode\": 12345\n"
          + "  },\n"
          + "  \"projects\": [         // Array of objects\n"
          + "    { \"title\": \"Project Alpha\", \"id\": \"PRJ001\" },\n"
          + "    { \"title\": \"Website Redesign\", \"id\": \"PRJ002\" }\n"
          + "  ]\n"
          + "}";
  private static final JsonSchemaInferrer JSON_SCHEMA_INFERRER =
      JsonSchemaInferrer.newBuilder()
          .setSpecVersion(SpecVersion.DRAFT_06)
          .setAdditionalPropertiesPolicy(AdditionalPropertiesPolicies.notAllowed())
          .setRequiredPolicy(RequiredPolicies.nonNullCommonFields())
          .build();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String JSON_SCHEMA;

  static {
    try {
      JSON_SCHEMA = JSON_SCHEMA_INFERRER.inferForSample(MAPPER.readTree(JSON_OBJECT)).toString();
    } catch (JsonProcessingException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Test
  public void testAvroSchema() {
    var schema = JsonSchema.avroSchemaFromJsonSchema(JSON_SCHEMA);
    System.out.println(schema.toString());
  }
}
