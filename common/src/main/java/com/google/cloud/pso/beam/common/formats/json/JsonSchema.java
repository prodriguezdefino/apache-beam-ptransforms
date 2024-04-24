package com.google.cloud.pso.beam.common.formats.json;

import java.util.HashMap;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.StringSchema;
import org.json.JSONObject;

/** */
public class JsonSchema {

  public static Schema avroSchemaFromJsonSchema(String jsonSchemaStr) {
    return avroSchemaFromJsonSchema(jsonSchemaFromString(jsonSchemaStr), null);
  }

  private static ObjectSchema jsonSchemaFromString(String jsonSchema) {
    JSONObject parsedSchema = new JSONObject(jsonSchema);
    org.everit.json.schema.Schema schemaValidator =
        org.everit.json.schema.loader.SchemaLoader.load(parsedSchema);
    if (!(schemaValidator instanceof ObjectSchema)) {
      throw new IllegalArgumentException(
          String.format("The schema is not a valid object schema:%n %s", jsonSchema));
    }
    return (org.everit.json.schema.ObjectSchema) schemaValidator;
  }

  static Schema avroSchemaFromJsonSchema(ObjectSchema jsonSchema, String recordName) {
    var avroSchemaBuilder =
        SchemaBuilder.record(Optional.ofNullable(recordName).orElse("topLevelRecord")).fields();
    var properties =
        new HashMap<String, org.everit.json.schema.Schema>(jsonSchema.getPropertySchemas());
    // Properties in a JSON Schema are stored in a Map object and unfortunately don't maintain
    // order. However, the schema's required properties is a list of property names that is
    // consistent and is in the same order as when the schema was first created. To create a
    // consistent Beam Schema from the same JSON schema, we add Schema Fields following this order.
    // We can guarantee a consistent Beam schema when all JSON properties are required.
    for (var propertyName : jsonSchema.getRequiredProperties()) {
      var propertySchema = properties.get(propertyName);
      if (propertySchema == null) {
        throw new IllegalArgumentException("Unable to parse schema " + jsonSchema);
      }

      Boolean isNullable =
          Boolean.TRUE.equals(propertySchema.getUnprocessedProperties().get("nullable"));
      avroSchemaBuilder =
          addPropertySchemaToRecordSchema(
              propertyName, propertySchema, avroSchemaBuilder, isNullable);
      // Remove properties we already added.
      properties.remove(propertyName, propertySchema);
    }

    // Now we are potentially left with properties that are not required. Add them too.
    // Note: having more than one non-required properties may result in  inconsistent
    // Beam schema field orderings.
    for (var entry : properties.entrySet()) {
      String propertyName = entry.getKey();
      org.everit.json.schema.Schema propertySchema = entry.getValue();

      if (propertySchema == null) {
        throw new IllegalArgumentException("Unable to parse schema " + jsonSchema);
      }
      // Consider non-required properties to be nullable
      avroSchemaBuilder =
          addPropertySchemaToRecordSchema(propertyName, propertySchema, avroSchemaBuilder, true);
    }

    return avroSchemaBuilder.endRecord();
  }

  private static SchemaBuilder.FieldAssembler<Schema> addPropertySchemaToRecordSchema(
      String propertyName,
      org.everit.json.schema.Schema propertySchema,
      SchemaBuilder.FieldAssembler<Schema> recordFieldsAssembler,
      Boolean isNullable) {
    if (propertySchema instanceof ArraySchema arraySchema) {
      if (arraySchema.getAllItemSchema() == null) {
        throw new IllegalArgumentException(
            "Tuple-like arrays are unsupported. Expected a single item type for field "
                + propertyName);
      }
      var baseArray = recordFieldsAssembler.name(propertyName).type().array();
      var elementSchema =
          avroSchemaFromJsonSchemaType(arraySchema.getAllItemSchema(), propertyName, isNullable);
      var arrayDefault =
          isNullable
              ? baseArray.items().nullable().type(elementSchema)
              : baseArray.items(elementSchema);
      // no default support for now
      return arrayDefault.noDefault();
    } else {
      try {
        var fieldSchema = avroSchemaFromJsonSchemaType(propertySchema, propertyName, isNullable);
        var baseField = recordFieldsAssembler.name(propertyName);
        // no default support for now
        return baseField.type(fieldSchema).noDefault();
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            "Unsupported field type " + propertySchema.getClass() + " in field " + propertyName, e);
      }
    }
  }

  private static Schema avroSchemaFromJsonSchemaType(
      org.everit.json.schema.Schema propertySchema, String propertyName, boolean isNullable) {
    var builder = isNullable ? SchemaBuilder.nullable() : SchemaBuilder.builder();

    if (propertySchema instanceof ObjectSchema objectSchema) {
      return builder.type(avroSchemaFromJsonSchema(objectSchema, propertyName));
    } else if (propertySchema instanceof BooleanSchema) {
      return builder.booleanType();
    } else if (propertySchema instanceof NumberSchema numberSchema) {
      return numberSchema.requiresInteger() ? builder.longType() : builder.doubleType();
    } else if (propertySchema instanceof StringSchema) {
      return builder.stringType();
    } else if (propertySchema instanceof ReferenceSchema refSchema) {
      return avroSchemaFromJsonSchemaType(refSchema.getReferredSchema(), propertyName, isNullable);
    } else if (propertySchema instanceof ArraySchema arraySchema) {
      if (arraySchema.getAllItemSchema() == null) {
        throw new IllegalArgumentException(
            "Array schema is not properly formatted or unsupported ("
                + propertySchema
                + "). Note that JSON-schema's tuple-like arrays are not supported by Beam.");
      }
      return builder
          .array()
          .items(
              avroSchemaFromJsonSchemaType(
                  arraySchema.getAllItemSchema(), propertyName, isNullable));
    } else {
      throw new IllegalArgumentException("Unsupported schema type: " + propertySchema.getClass());
    }
  }
}
