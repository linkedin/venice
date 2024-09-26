package com.linkedin.venice.utils;

import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.avroutil1.compatibility.AvroIncompatibleSchemaException;
import com.linkedin.avroutil1.compatibility.AvroSchemaVerifier;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaEntry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.io.parsing.Symbol;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;


public class AvroSchemaUtils {
  private AvroSchemaUtils() {
    // Utility class.
  }

  private static final String NAMESPACE_FIELD = "namespace";

  /**
   * Filter the given schemas using the referenceSchema and AvroCompatibilityHelper. The helper compares the
   * canonicalized version of the schemas which means some differences are ignored when comparing two schemas.
   * Specifically things docs and at the time of writing, default values (which is a bug).
   *
   * @param referenceSchema used to find matching schema(s).
   * @param schemas to be filtered.
   * @return
   */
  public static List<SchemaEntry> filterCanonicalizedSchemas(
      SchemaEntry referenceSchema,
      Collection<SchemaEntry> schemas) {
    List<SchemaEntry> results = new ArrayList<>();
    String cannonicalizedReferenceSchema = AvroCompatibilityHelper.toParsingForm(referenceSchema.getSchema());
    for (SchemaEntry entry: schemas) {
      if (cannonicalizedReferenceSchema.equals(AvroCompatibilityHelper.toParsingForm(entry.getSchema())))
        results.add(entry);
    }
    return results;
  }

  /**
   * It verifies that the schema's union field default value must be same type as the first field.
   * From https://avro.apache.org/docs/current/spec.html#Unions
   * (Note that when a default value is specified for a record field whose type is a union, the type of the
   * default value must match the first element of the union. Thus, for unions containing "null", the "null" is usually
   * listed first, since the default value of such unions is typically null.)
   * @param str
   */
  public static void validateAvroSchemaStr(String str) {
    Schema schema = Schema.parse(str);
    try {
      validateAvroSchemaStr(schema);
    } catch (AvroIncompatibleSchemaException e) {
      // Wrap and throw to propagate an ENUM error type to the user
      throw new InvalidVeniceSchemaException(e.getMessage(), e);
    }
  }

  public static boolean isValidAvroSchema(Schema schema) {
    try {
      validateAvroSchemaStr(schema);
    } catch (AvroIncompatibleSchemaException exception) {
      return false;
    }
    return true;
  }

  public static void validateAvroSchemaStr(Schema schema) {
    AvroSchemaVerifier.get().verifyCompatibility(schema, schema);
  }

  /**
   * Filter the given schemas using the referenceSchema and the underlying {@code Schema.equals} method.
   * @param referenceSchema
   * @param schemas
   * @return
   */
  public static List<SchemaEntry> filterSchemas(SchemaEntry referenceSchema, Collection<SchemaEntry> schemas) {
    List<SchemaEntry> results = new ArrayList<>();
    for (SchemaEntry entry: schemas) {
      if (referenceSchema.getSchema().equals(entry.getSchema())
          && !hasDocFieldChange(referenceSchema.getSchema(), entry.getSchema())) {
        results.add(entry);
      }
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
    return hasErrors(symbol);
  }

  /**
   * Extracted from avro-1.7.7 avro/io/parsing/Symbol.java to work with legacy avro versions
   */
  private static boolean hasErrors(Symbol symbol) {
    switch (symbol.kind) {
      case ALTERNATIVE:
        return hasErrors(symbol, ((Symbol.Alternative) symbol).symbols);
      case EXPLICIT_ACTION:
      case TERMINAL:
        return false;
      case IMPLICIT_ACTION:
        return symbol instanceof Symbol.ErrorAction;
      case REPEATER:
        Symbol.Repeater r = (Symbol.Repeater) symbol;
        return hasErrors(r.end) || hasErrors(symbol, r.production);
      case ROOT:
      case SEQUENCE:
        return hasErrors(symbol, symbol.production);
      default:
        throw new RuntimeException("unknown symbol kind: " + symbol.kind);
    }
  }

  /**
   * Extracted from avro-1.7.7 avro/io/parsing/Symbol.java to work with legacy avro versions
   */
  private static boolean hasErrors(Symbol root, Symbol[] symbols) {
    if (symbols != null) {
      for (Symbol s: symbols) {
        if (s == root) {
          continue;
        }
        if (hasErrors(s)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Generate a new schema based on the provided schema string with the namespace specified by {@param namespace}.
   * @param schemaStr is the original string of the writer schema. This is because string -> avro schema -> string
   *                  may not give back the original schema string.
   * @param namespace the desired namespace for the generated schema.
   * @return a new {@link Schema} with the specified namespace.
   */
  public static Schema generateSchemaWithNamespace(String schemaStr, String namespace) throws IOException {
    ObjectMapper mapper = ObjectMapperFactory.getInstance();
    ObjectNode schemaOb = mapper.readValue(schemaStr, ObjectNode.class);
    schemaOb.put(NAMESPACE_FIELD, namespace);
    return Schema.parse(schemaOb.toString());
  }

  /**
   * Compares two schema with possible re-ordering of the fields. Otherwise, If compares every field at every level.
   * @param s1
   * @param s2
   * @return true is the schemas are same with possible reordered fields.
   */
  public static boolean compareSchemaIgnoreFieldOrder(Schema s1, Schema s2) {
    if (s1.getType() != s2.getType()) {
      return false;
    }

    // Special handling for String vs Avro string comparison
    if (Objects.equals(s1, s2) || s1.getType() == Schema.Type.STRING) {
      return true;
    }
    switch (s1.getType()) {
      case RECORD:
        return StringUtils.equals(s1.getNamespace(), s2.getNamespace()) && compareFields(s1, s2);
      case ARRAY:
        return compareSchemaIgnoreFieldOrder(s1.getElementType(), s2.getElementType());
      case MAP:
        return compareSchemaIgnoreFieldOrder(s1.getValueType(), s2.getValueType());
      case UNION:
        return compareSchemaUnion(s1.getTypes(), s2.getTypes());
      case ENUM:
        return compareSchemaEnum(s1.getEnumSymbols(), s2.getEnumSymbols());
      default:
        throw new VeniceException("Schema compare not supported for " + s1.toString());
    }
  }

  private static boolean compareSchemaEnum(List<String> list1, List<String> list2) {
    return list1.equals(list2);
  }

  private static boolean compareSchemaUnion(List<Schema> list1, List<Schema> list2) {
    Map<String, Schema> s2Schema = list2.stream().collect(Collectors.toMap(s -> s.getName(), s -> s));
    for (Schema s1: list1) {
      Schema s2 = s2Schema.get(s1.getName());
      if (s2 == null || !compareSchemaIgnoreFieldOrder(s1, s2)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Non-throwing flavor of AvroCompatibilityHelper.getGenericDefaultValue, returns null if there is no default.
   */
  @Nullable
  public static Object getFieldDefault(Schema.Field field) {
    return AvroCompatibilityHelper.fieldHasDefault(field)
        ? AvroCompatibilityHelper.getNullableGenericDefaultValue(field)
        : null;
  }

  private static boolean compareFields(Schema s1, Schema s2) {
    if (s1.getFields().size() != s2.getFields().size()) {
      return false;
    }

    for (Schema.Field f1: s1.getFields()) {
      Schema.Field f2 = s2.getField(f1.name());
      if (f2 == null || !Objects.equals(getFieldDefault(f1), getFieldDefault(f2))
          || !compareSchemaIgnoreFieldOrder(f1.schema(), f2.schema())) {
        return false;
      }
    }
    return true;
  }

  public static SchemaEntry generateSupersetSchemaFromAllValueSchemas(Collection<SchemaEntry> allValueSchemaEntries) {
    if (allValueSchemaEntries.isEmpty()) {
      throw new IllegalArgumentException("Value schema list cannot be empty.");
    }
    if (allValueSchemaEntries.size() == 1) {
      return allValueSchemaEntries.iterator().next();
    }
    List<SchemaEntry> valueSchemaEntries = new ArrayList<>(allValueSchemaEntries);
    Schema tmpSupersetSchema = valueSchemaEntries.get(0).getSchema();
    int largestSchemaID = valueSchemaEntries.get(0).getId();

    for (int i = 1; i < valueSchemaEntries.size(); i++) {
      final SchemaEntry valueSchemaEntry = valueSchemaEntries.get(i);
      final Schema valueSchema = valueSchemaEntry.getSchema();
      validateTwoSchemasAreFullyCompatible(tmpSupersetSchema, valueSchema);
      largestSchemaID = Math.max(largestSchemaID, valueSchemaEntry.getId());
      /**
       * Current superset schema should be the first parameter, and the incoming value schema is the 2nd parameter.
       */
      tmpSupersetSchema = AvroSupersetSchemaUtils.generateSupersetSchema(tmpSupersetSchema, valueSchema);
    }
    final Schema supersetSchema = tmpSupersetSchema;

    // Check whether one of the existing schemas is the same as the generated superset schema.
    SchemaEntry existingSupersetSchemaEntry = null;
    for (SchemaEntry valueSchemaEntry: valueSchemaEntries) {
      if (AvroSchemaUtils.compareSchemaIgnoreFieldOrder(valueSchemaEntry.getSchema(), supersetSchema)) {
        existingSupersetSchemaEntry = valueSchemaEntry;
        break;
      }
    }
    if (existingSupersetSchemaEntry == null) {
      final int supersetSchemaID = largestSchemaID + 1;
      return new SchemaEntry(supersetSchemaID, supersetSchema);

    } else {
      return existingSupersetSchemaEntry;
    }
  }

  private static void validateTwoSchemasAreFullyCompatible(Schema schema1, Schema schema2) {
    SchemaCompatibility.SchemaPairCompatibility compatibility =
        SchemaCompatibility.checkReaderWriterCompatibility(schema1, schema2);
    if (compatibility.getType() != SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE) {
      throw new VeniceException("Expect all value schemas to be fully compatible. Got: " + schema1 + " and " + schema2);
    }
    compatibility = SchemaCompatibility.checkReaderWriterCompatibility(schema2, schema1);
    if (compatibility.getType() != SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE) {
      throw new VeniceException("Expect all value schemas to be fully compatible. Got: " + schema1 + " and " + schema2);
    }
  }

  /**
   * Generate superset schema of two Schemas. If we have {A,B,C} and {A,B,D} it will generate {A,B,C,D}, where
   * C/D could be nested record change as well eg, array/map of records, or record of records.
   * Prerequisite: The top-level schema are of type RECORD only and each field have default values. ie they are compatible
   * schemas and the generated schema will pick the default value from new schema.
   *
   * @param existingSchema Existing schema
   * @param newSchema New schema
   * @return Superset schema of existing and new schemas.
   */

  // Visible for testing.

  /**
   * Given s1 and s2 returned SchemaEntry#equals(s1,s2) true, verify they have doc field change.
   * It assumes rest of the fields are exactly same. DO NOT USE this to compare schemas.
   * @param s1
   * @param s2
   * @return true if s1 and s2 has differences in doc field when checked recursively.
   *         false if s1 and s2 are exactly same including the doc (but does not check for strict equality).
   */
  public static boolean hasDocFieldChange(Schema s1, Schema s2) {
    if (!StringUtils.equals(s1.getDoc(), s2.getDoc())) {
      return true;
    }

    switch (s1.getType()) {
      case RECORD:
        return hasDocFieldChangeFields(s1, s2);
      case ARRAY:
        return hasDocFieldChangeFields(s1.getElementType(), s2.getElementType());
      case MAP:
        return hasDocFieldChangeFields(s1.getValueType(), s2.getValueType());
      case UNION:
        return hasDocFieldChangeFieldsUnion(s1.getTypes(), s2.getTypes());
      default:
        return false;
    }
  }

  private static boolean hasDocFieldChangeFieldsUnion(List<Schema> list1, List<Schema> list2) {
    Map<String, Schema> s2Schema = list2.stream().collect(Collectors.toMap(s -> s.getName(), s -> s));
    for (Schema s1: list1) {
      Schema s2 = s2Schema.get(s1.getName());
      if (s2 == null) {
        throw new VeniceException("Schemas dont match!");
      }
      if (hasDocFieldChange(s1, s2)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasDocFieldChangeFields(Schema s1, Schema s2) {
    if (!StringUtils.equals(s1.getDoc(), s2.getDoc())) {
      return true;
    }

    // Recurse down for RECORD type schemas only. s1 and s2 have same schema type.
    if (s1.getType() == RECORD) {
      for (Schema.Field f1: s1.getFields()) {
        Schema.Field f2 = s2.getField(f1.name());
        if (f2 == null) {
          throw new VeniceException("Schemas dont match!");
        }
        if (!StringUtils.equals(f1.doc(), f2.doc())) {
          return true;
        }
        if (hasDocFieldChange(f1.schema(), f2.schema())) {
          return true;
        }
      }
    }
    return false;
  }

  public static void validateTopLevelFieldDefaultsValueRecordSchema(Schema valueRecordSchema) {
    Validate.notNull(valueRecordSchema);
    if (valueRecordSchema.getType() != RECORD) {
      return;
    }
    for (Schema.Field field: valueRecordSchema.getFields()) {
      if (!AvroCompatibilityHelper.fieldHasDefault(field)) {
        throw new IllegalArgumentException(
            "Top-level field " + field.name() + " is missing default. Value schema: " + valueRecordSchema);
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

    return types.get(0).getType() == NULL || types.get(1).getType() == NULL;
  }

  public static Schema createFlattenedUnionSchema(List<Schema> schemasInUnion) {
    List<Schema> flattenedSchemaList = new ArrayList<>(schemasInUnion.size());
    for (Schema schemaInUnion: schemasInUnion) {
      // if the origin schema is union, we'd like to flatten it
      // we don't need to do it recursively because Avro doesn't support nested union
      if (schemaInUnion.getType() == UNION) {
        flattenedSchemaList.addAll(schemaInUnion.getTypes());
      } else {
        flattenedSchemaList.add(schemaInUnion);
      }
    }

    return Schema.createUnion(flattenedSchemaList);
  }

  /**
   * Create a {@link GenericRecord} from a given schema. The created record has default values set on all fields. Note
   * that all fields in the given schema must have default values. Otherwise, an exception is thrown.
   */
  public static GenericRecord createGenericRecord(Schema originalSchema) {
    final GenericData.Record newRecord = new GenericData.Record(originalSchema);
    for (Schema.Field originalField: originalSchema.getFields()) {
      if (AvroCompatibilityHelper.fieldHasDefault(originalField)) {
        // make a deep copy here since genericData caches each default value internally. If we
        // use what it returns, we will mutate the cache.
        newRecord.put(
            originalField.pos(),
            GenericData.get()
                .deepCopy(originalField.schema(), AvroCompatibilityHelper.getGenericDefaultValue(originalField)));
      } else {
        throw new VeniceException(
            String.format(
                "Cannot apply updates because Field: %s is null and " + "default value is not defined",
                originalField.name()));
      }
    }

    return newRecord;
  }

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
    for (Schema type: types) {
      switch (type.getType()) {
        case ARRAY:
        case MAP:
          if (hasCollectionType) {
            // More than one collection type found, this won't work.
            throw new VeniceException(
                "Multiple collection types in a union are not allowedSchema: " + unionSchema.toString(true));
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
   * @return true if UnresolvedUnionException is available in the Avro version on the classpath, or false otherwise
   */
  public static boolean isUnresolvedUnionExceptionAvailable() {
    return AvroCompatibilityHelperCommon.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_4);
  }
}
