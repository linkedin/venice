package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.List;
import org.apache.avro.Schema;

import static com.linkedin.venice.schema.WriteComputeSchemaAdapter.WriteComputeOperation.*;
import static org.apache.avro.Schema.Type.*;

/**
 * validate if a write-compute schema can be pair with value schema
 */
public class WriteComputeSchemaValidator {
  public static void validate(Schema originalSchema, Schema writeComputeSchema) {
    if (originalSchema.getType() != RECORD) {
      throw new InvalidWriteComputeException("write compute is only supported for generic record");
    }

    boolean isValidSchema = validateSchema(originalSchema, writeComputeSchema);
    if (!isValidSchema) {
      throw new InvalidWriteComputeException(originalSchema, writeComputeSchema);
    }
  }

  private static boolean validateSchema(Schema originalSchema, Schema writeComputeSchema) {
    switch (originalSchema.getType()) {
      case RECORD:
        return validateRecord(originalSchema, writeComputeSchema);
      case ARRAY:
        return validateArray(originalSchema, writeComputeSchema);
      case MAP:
        return validateMap(originalSchema, writeComputeSchema);
      case UNION:
        return validateUnion(originalSchema, writeComputeSchema);
      default:
        return originalSchema.equals(writeComputeSchema);
    }
  }

  private static boolean validateRecord(Schema originalSchema, Schema writeComputeSchema) {
    if (writeComputeSchema.getType() != RECORD) {
      //If writeComputeSchema is a union type and contains DelOp, recurse on initial record
      if (writeComputeSchema.getType() == UNION && writeComputeSchema.getTypes().get(1).getName().equals(DEL_OP.name)) {
        return validateSchema(originalSchema, writeComputeSchema.getTypes().get(0));
      }
      return false;
    }

    for (Schema.Field field : originalSchema.getFields()) {
      Schema writeComputeFieldSchema = writeComputeSchema.getField(field.name()).schema();
      if (writeComputeFieldSchema == null) {
        return false;
      }

      if (writeComputeFieldSchema.getType() != UNION) {
        return false;
      }

      List<Schema> unionSubTypes = writeComputeFieldSchema.getTypes();
      if (!unionSubTypes.get(0).getName().equals(NO_OP.name)) {
        return false;
      }

      //strip out NoOp operation before passing it to next layer.
      //This will make the logic consistent for both non-field schema
      // and schema in inside the field.
      Schema subTypesSchema;
      List<Schema> subtypesWithoutNoOp = unionSubTypes.subList(1, unionSubTypes.size());
      if (subtypesWithoutNoOp.size() == 1) {
        subTypesSchema = subtypesWithoutNoOp.get(0);
      } else {
        subTypesSchema = Schema.createUnion(subtypesWithoutNoOp);
      }
      if (!validateSchema(field.schema(), subTypesSchema)) {
        return false;
      }
    }

    return true;
  }

  private static boolean validateArray(Schema originalSchema, Schema writeComputeSchema) {
    return validateCollectionSchema(originalSchema, writeComputeSchema, LIST_OPS);
  }

  private static boolean validateMap(Schema originalSchema, Schema writeComputeSchema) {
    return validateCollectionSchema(originalSchema, writeComputeSchema, MAP_OPS);
  }

  private static boolean validateCollectionSchema(Schema originalSchema, Schema writeComputeSchema,
      WriteComputeSchemaAdapter.WriteComputeOperation operation) {
    List<Schema> unionSubTypes = writeComputeSchema.getTypes();
    if (unionSubTypes.size() != 2) {
      return false;
    }

    if (unionSubTypes.get(0).getType() != RECORD ||
        !unionSubTypes.get(0).getName().toLowerCase().endsWith(operation.name.toLowerCase())) {
      return false;
    }

    return originalSchema.equals(unionSubTypes.get(1));
  }

  private static boolean validateUnion(Schema originalSchema, Schema writeComputeSchema) {
    List<Schema> originalSubSchemas = originalSchema.getTypes();
    List<Schema> writeComputeSubSchemas = writeComputeSchema.getTypes();

    if (originalSubSchemas.size() != writeComputeSubSchemas.size()) {
      return false;
    }

    for (int i = 0; i < originalSubSchemas.size(); i ++) {
      if (!validateSchema(originalSubSchemas.get(i), writeComputeSubSchemas.get(i))) {
        return false;
      }
    }

    return true;
  }

  public static class InvalidWriteComputeException extends VeniceException {
    InvalidWriteComputeException(String msg) {
      super(msg);
    }
    InvalidWriteComputeException(Schema originalSchema, Schema writeComputeSchema) {
      super(String.format("write compute schema is inconsistent with original schema.\n" +
              "original schema: \n%s\n" + "write compute schema: \n%s", originalSchema.toString(true),
          writeComputeSchema.toString(true)));
    }
  }
}
