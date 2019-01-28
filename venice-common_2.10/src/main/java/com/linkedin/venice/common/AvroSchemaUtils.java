package com.linkedin.venice.common;

import com.linkedin.venice.schema.SchemaEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.avro.io.LinkedinAvroMigrationHelper;


public class AvroSchemaUtils {

  /**
   * Filter the given schemas using the referenceSchema and LinkedinAvroMigrationHelper. The helper compares the
   * canonicalized version of the schemas which means some differences are ignored when comparing two schemas.
   * Specifically things docs and at the time of writing, default values (which is a bug).
   *
   * @param referenceSchema used to find matching schema(s).
   * @param schemas to be filtered.
   * @return
   */
  public static List<SchemaEntry> filterCanonicalizedSchemas (SchemaEntry referenceSchema,
      Collection<SchemaEntry> schemas) {
    List<SchemaEntry> results = new ArrayList<>();
    String cannonicalizedReferenceSchema = LinkedinAvroMigrationHelper.toParsingForm(referenceSchema.getSchema());
    for (SchemaEntry entry : schemas) {
      if (cannonicalizedReferenceSchema.equals(LinkedinAvroMigrationHelper.toParsingForm(entry.getSchema())))
        results.add(entry);
    }
    return results;
  }

  /**
   * Filter the given schemas using the referenceSchema and the underlying {@code Schema.equals} method.
   * @param referenceSchema
   * @param schemas
   * @return
   */
  public static List<SchemaEntry> filterSchemas (SchemaEntry referenceSchema, Collection<SchemaEntry> schemas) {
    List<SchemaEntry> results = new ArrayList<>();
    for (SchemaEntry entry : schemas) {
      if (referenceSchema.getSchema().equals(entry.getSchema()))
        results.add(entry);
    }
    return results;
  }
}
