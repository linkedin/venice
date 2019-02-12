package com.linkedin.venice.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.venice.schema.SchemaEntry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.io.LinkedinAvroMigrationHelper;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.io.parsing.Symbol;


public class AvroSchemaUtils {

  private static final String NAMESPACE_FIELD = "namespace";

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

  /**
   * Preemptive check to see if the given writer and reader schema can be resolved without errors.
   * @param writerSchema is the schema used when serializing the object.
   * @param readerSchema is the schema used when deserializing the object.
   * @return {@code boolean} that indicated if there were errors.
   * @throws IOException
   */
  public static boolean schemaResolveHasErrors(Schema writerSchema, Schema readerSchema) throws IOException {
    Symbol symbol = (Symbol) ResolvingDecoder.resolve(writerSchema, readerSchema);
    return Symbol.hasErrors(symbol);
  }

  /**
   * Generate a new schema based on the provided schema string with the namespace specified by {@param namespace}.
   * @param schemaStr is the original string of the writer schema. This is because string -> avro schema -> string
   *                  may not give back the original schema string.
   * @param namespace the desired namespace for the generated schema.
   * @return a new {@link Schema} with the specified namespace.
   */
  public static Schema generateSchemaWithNamespace(String schemaStr, String namespace) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode schemaOb = mapper.readValue(schemaStr, ObjectNode.class);
    schemaOb.put(NAMESPACE_FIELD, namespace);
    return Schema.parse(schemaOb.toString());
  }
}
