package com.linkedin.venice.etl;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.vson.VsonAvroSchemaAdapter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;


public class ETLUtils {
  /**
   * Modify a store value schema to the value schema that can be used for ETL by allowing "null" values to be set
   * @param valueSchema The store's value schema
   * @return A schema that can be used for ETL by allowing "null" values to be set
   */
  public static Schema transformValueSchemaForETL(Schema valueSchema) {
    ETLValueSchemaTransformation transformation = ETLValueSchemaTransformation.fromSchema(valueSchema);
    Schema etlValueSchema;

    switch (transformation) {
      case UNIONIZE_WITH_NULL:
        etlValueSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), valueSchema));
        break;
      case ADD_NULL_TO_UNION:
        List<Schema> schemasInUnion = valueSchema.getTypes();
        List<Schema> etlValueSchemaTypes = new ArrayList<>();
        etlValueSchemaTypes.add(0, Schema.create(Schema.Type.NULL));
        etlValueSchemaTypes.addAll(schemasInUnion);
        etlValueSchema = Schema.createUnion(etlValueSchemaTypes);
        break;
      case NONE:
        etlValueSchema = valueSchema;
        break;
      default:
        throw new VeniceException("Invalid ETL Value schema transformation: " + transformation);
    }

    return etlValueSchema;
  }

  /**
   * Get the store's value schema from it's value schema in the ETL output and the transformation that was applied to
   * construct it.
   * @param etlValueSchema Schema of the "value" field in the ETL data.
   * @param transformation The transformation that was applied to construct the schema of the "value" field in ETL data from the store's value schema.
   * @return The store's value schema that would have been used to construct the schema of "value" field in ETL data.
   */
  public static Schema getValueSchemaFromETLValueSchema(
      Schema etlValueSchema,
      ETLValueSchemaTransformation transformation) {
    Schema pushValueSchema;

    switch (transformation) {
      case UNIONIZE_WITH_NULL:
        pushValueSchema = VsonAvroSchemaAdapter.stripFromUnion(etlValueSchema);
        break;
      case ADD_NULL_TO_UNION:
        List<Schema> schemasInUnion = etlValueSchema.getTypes();
        List<Schema> nullStrippedUnionSchema = schemasInUnion.stream()
            .filter(schema -> !schema.getType().equals(Schema.Type.NULL))
            .collect(Collectors.toList());
        pushValueSchema = Schema.createUnion(nullStrippedUnionSchema);
        break;
      case NONE:
        pushValueSchema = etlValueSchema;
        break;
      default:
        throw new VeniceException("Invalid ETL Value schema transformation: " + transformation);
    }

    return pushValueSchema;
  }
}
