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
    return validateSchema(originalSchema, writeComputeSchema, false);
  }

  private static boolean validateSchema(Schema originalSchema, Schema writeComputeSchema, boolean isNestedField) {
    switch (originalSchema.getType()) {
      case RECORD:
        return validateRecord(originalSchema, writeComputeSchema, isNestedField);
      case ARRAY:
      case MAP:
        return validateCollectionSchema(originalSchema, writeComputeSchema);
      case UNION:
        return validateUnion(originalSchema, writeComputeSchema);
      default:
        return originalSchema.equals(writeComputeSchema);
    }
  }

  private static boolean validateRecord(Schema originalSchema, Schema writeComputeSchema, boolean isNestedField) {
    if (isNestedField) {
      return originalSchema.equals(writeComputeSchema);
    }
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

  private static boolean validateCollectionSchema(Schema originalSchema, Schema writeComputeSchema) {
    WriteComputeSchemaAdapter.WriteComputeOperation operation;
    if (originalSchema.getType() == ARRAY) {
      operation = LIST_OPS;
    } else if (originalSchema.getType() == MAP) {
      operation = MAP_OPS;
    } else {
      //defensive code. Shouldn't be here
      throw new VeniceException("The write compute schema is not a collection type schema." +
          writeComputeSchema.toString(true));
    }

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

    int writeComputeSchemaIndex = 0;
    for (int originalSchemaIndex = 0; originalSchemaIndex < originalSubSchemas.size();
        originalSchemaIndex ++, writeComputeSchemaIndex ++) {
      try {
        //Validate the element's type one by one. If union contains collectional
        //elements, it will validate both the elements and available operations.
        if (originalSubSchemas.get(originalSchemaIndex).getType() == ARRAY || originalSubSchemas.get(originalSchemaIndex).getType() == MAP) {
          Schema collectionSchemaUnion = Schema.createUnion(writeComputeSubSchemas.subList(writeComputeSchemaIndex, writeComputeSchemaIndex + 2));
          if (!validateCollectionSchema(originalSubSchemas.get(originalSchemaIndex), collectionSchemaUnion)) {
            return false;
          }

          writeComputeSchemaIndex++;
        } else {
          if (!validateSchema(originalSubSchemas.get(originalSchemaIndex), writeComputeSubSchemas.get(writeComputeSchemaIndex), true)) {
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
      super(String.format("write compute schema is inconsistent with original schema.\n" +
              "original schema: \n%s\n" + "write compute schema: \n%s", originalSchema.toString(true),
          writeComputeSchema.toString(true)));
    }
  }
}
