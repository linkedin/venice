package com.linkedin.venice.schema.writecompute;

import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.LIST_OPS;
import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.MAP_OPS;
import static com.linkedin.venice.schema.writecompute.WriteComputeOperation.NO_OP_ON_FIELD;
import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.List;
import org.apache.avro.Schema;


/**
 * validate if a write-compute schema can be pair with value schema
 */
public class WriteComputeSchemaValidator {
  private WriteComputeSchemaValidator() {
    // Utility class
  }

  public static void validate(Schema valueSchema, Schema writeComputeSchema) {
    if (valueSchema.getType() != RECORD) {
      throw new InvalidWriteComputeException("write compute is only supported for generic record");
    }

    boolean isValidSchema = validateSchema(valueSchema, writeComputeSchema);
    if (!isValidSchema) {
      throw new InvalidWriteComputeException(valueSchema, writeComputeSchema);
    }
  }

  private static boolean validateSchema(Schema valueSchema, Schema writeComputeSchema) {
    return validateSchema(valueSchema, writeComputeSchema, false);
  }

  private static boolean validateSchema(Schema valueSchema, Schema writeComputeSchema, boolean isNestedField) {
    switch (valueSchema.getType()) {
      case RECORD:
        return validateRecord(valueSchema, writeComputeSchema, isNestedField);
      case ARRAY:
      case MAP:
        return validateCollectionSchema(valueSchema, writeComputeSchema);
      case UNION:
        return validateUnion(valueSchema, writeComputeSchema);
      default:
        return valueSchema.equals(writeComputeSchema);
    }
  }

  private static boolean validateRecord(Schema valueSchema, Schema writeComputeSchema, boolean isNestedField) {
    if (isNestedField) {
      return valueSchema.equals(writeComputeSchema);
    }
    if (writeComputeSchema.getType() != RECORD) {
      if (writeComputeSchema.getType() == UNION) {
        /**
         * If writeComputeSchema is a union schema, recurse on the first branch in the union. This is to handle legacy
         * Write Compute schemas that are union of Update and Delete schemas. The first branch in the union schema should
         * be the Update schema.
         */
        Schema updateSchema = writeComputeSchema.getTypes().get(0);
        return validateSchema(valueSchema, updateSchema, false);
      } else {
        return false;
      }
    }

    for (Schema.Field field: valueSchema.getFields()) {
      Schema writeComputeFieldSchema = writeComputeSchema.getField(field.name()).schema();
      if (writeComputeFieldSchema == null) {
        return false;
      }

      if (writeComputeFieldSchema.getType() != UNION) {
        return false;
      }

      List<Schema> unionSubTypes = writeComputeFieldSchema.getTypes();
      if (!unionSubTypes.get(0).getName().equals(NO_OP_ON_FIELD.name)) {
        return false;
      }

      // strip out NoOp operation before passing it to next layer.
      // This will make the logic consistent for both non-field schema
      // and schema in inside the field.
      Schema subTypesSchema;
      List<Schema> subtypesWithoutNoOp = unionSubTypes.subList(1, unionSubTypes.size());
      if (subtypesWithoutNoOp.size() == 1) {
        subTypesSchema = subtypesWithoutNoOp.get(0);
      } else {
        subTypesSchema = Schema.createUnion(subtypesWithoutNoOp);
      }
      if (!validateSchema(field.schema(), subTypesSchema, true)) {
        return false;
      }
    }

    return true;
  }

  private static boolean validateCollectionSchema(Schema originalSchema, Schema writeComputeSchema) {
    WriteComputeOperation operation;
    if (originalSchema.getType() == ARRAY) {
      operation = LIST_OPS;
    } else if (originalSchema.getType() == MAP) {
      operation = MAP_OPS;
    } else {
      // defensive code. Shouldn't be here
      throw new VeniceException(
          "The write compute schema is not a collection type schema." + writeComputeSchema.toString(true));
    }

    List<Schema> unionSubTypes = writeComputeSchema.getTypes();
    if (unionSubTypes.size() != 2) {
      return false;
    }

    if (unionSubTypes.get(0).getType() != RECORD
        || !unionSubTypes.get(0).getName().toLowerCase().endsWith(operation.name.toLowerCase())) {
      return false;
    }

    return originalSchema.equals(unionSubTypes.get(1));
  }

  private static boolean validateUnion(Schema originalSchema, Schema writeComputeSchema) {
    List<Schema> originalSubSchemas = originalSchema.getTypes();
    List<Schema> writeComputeSubSchemas = writeComputeSchema.getTypes();

    int writeComputeSchemaIndex = 0;
    for (int originalSchemaIndex = 0; originalSchemaIndex < originalSubSchemas
        .size(); originalSchemaIndex++, writeComputeSchemaIndex++) {
      try {
        // Validate the element's type one by one. If union contains collectional
        // elements, it will validate both the elements and available operations.
        if (originalSubSchemas.get(originalSchemaIndex).getType() == ARRAY
            || originalSubSchemas.get(originalSchemaIndex).getType() == MAP) {
          Schema collectionSchemaUnion =
              Schema.createUnion(writeComputeSubSchemas.subList(writeComputeSchemaIndex, writeComputeSchemaIndex + 2));
          if (!validateCollectionSchema(originalSubSchemas.get(originalSchemaIndex), collectionSchemaUnion)) {
            return false;
          }

          writeComputeSchemaIndex++;
        } else {
          if (!validateSchema(
              originalSubSchemas.get(originalSchemaIndex),
              writeComputeSubSchemas.get(writeComputeSchemaIndex),
              true)) {
            return false;
          }
        }
      } catch (IndexOutOfBoundsException e) {
        return false;
      }
    }

    if (writeComputeSchemaIndex != writeComputeSubSchemas.size()) {
      return false;
    }

    return true;
  }

  public static class InvalidWriteComputeException extends VeniceException {
    InvalidWriteComputeException(String msg) {
      super(msg);
    }

    InvalidWriteComputeException(Schema originalSchema, Schema writeComputeSchema) {
      super(
          String.format(
              "write compute schema is inconsistent with original schema.\n" + "original schema: \n%s\n"
                  + "write compute schema: \n%s",
              originalSchema.toString(true),
              writeComputeSchema.toString(true)));
    }
  }
}
