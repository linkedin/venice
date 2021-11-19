package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.List;
import org.apache.avro.Schema;

public class DerivedSchemaUtils {
  /**
   * Utility function that checks to make sure that given a union schema, there only exists 1 collection type amongst the
   * provided types.  Multiple collections will make the result of the flattened write compute schema lead to ambiguous behavior
   *
   * @param unionSchema a union schema to validate.
   * @throws VeniceException When the unionSchema contains more then one collection type
   */
  public static void containsOnlyOneCollection(Schema unionSchema) {
    List<Schema> types = unionSchema.getTypes();
    boolean hasCollectionType = false;
    for (Schema type : types) {
      switch (type.getType()) {
        case ARRAY:
        case MAP:
          if (hasCollectionType) {
            // More then one collection type found, this won't work.
            throw new VeniceException("Multiple collection types in a union are not allowedSchema: "
                + unionSchema.toString(true));
          }
          hasCollectionType = true;
          continue;
        case RECORD:
        case UNION:
        default:
          continue;
      }
    }
  }

  /**
   * @param unionSchema
   * @return True iif the schema is of type UNION and it has 2 fields and one of them is NULL.
   */
  public static boolean isNullableUnionPair(Schema unionSchema) {
    if (unionSchema.getType() != Schema.Type.UNION) {
      return false;
    }
    List<Schema> types = unionSchema.getTypes();
    if (types.size() != 2) {
      return false;
    }

    return types.get(0).getType() == Schema.Type.NULL
        || types.get(1).getType() == Schema.Type.NULL;
  }
}
