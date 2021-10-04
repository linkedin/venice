package com.linkedin.venice.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroIncompatibleSchemaException;
import com.linkedin.avroutil1.compatibility.AvroSchemaVerifier;
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
import org.apache.avro.Schema;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.io.parsing.Symbol;
import org.apache.commons.lang.StringUtils;


public class AvroSchemaUtils {

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
  public static List<SchemaEntry> filterCanonicalizedSchemas(SchemaEntry referenceSchema,
      Collection<SchemaEntry> schemas) {
    List<SchemaEntry> results = new ArrayList<>();
    String cannonicalizedReferenceSchema = AvroCompatibilityHelper.toParsingForm(referenceSchema.getSchema());
    for (SchemaEntry entry : schemas) {
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
    for (SchemaEntry entry : schemas) {
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
    switch(symbol.kind) {
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
    if(null != symbols) {
      for(Symbol s: symbols) {
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
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode schemaOb = mapper.readValue(schemaStr, ObjectNode.class);
    schemaOb.put(NAMESPACE_FIELD, namespace);
    return Schema.parse(schemaOb.toString());
  }

  /**
   * Compares two schema with possible re-ordering of the fields. Otherwise If compares every fields at every level.
   * @param s1
   * @param s2
   * @return true is the schemas are same with possible reordered fields.
   */
  public static boolean compareSchemaIgnoreFieldOrder(Schema s1, Schema s2) {
    if (s1.getType() != s2.getType()) {
      return false;
    }

    // Special handling for String vs Avro string comparision
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
    for (Schema s1 : list1) {
      Schema s2 = s2Schema.get(s1.getName());
      if (s2 == null || !compareSchemaIgnoreFieldOrder(s1, s2)) {
        return false;
      }
    }
    return true;
  }

  private static boolean compareFields(Schema s1, Schema s2) {
    if (s1.getFields().size() != s2.getFields().size()) {
      return false;
    }

    for (Schema.Field f1 : s1.getFields()) {
      Schema.Field f2 = s2.getField(f1.name());
      if (f2  == null || !Objects.equals(f1.defaultValue(), f2.defaultValue()) || !compareSchemaIgnoreFieldOrder(f1.schema(), f2.schema())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Generate super-set schema of two Schemas. If we have {A,B,C} and {A,B,D} it will generate {A,B,C,D}, where
   * C/D could be nested record change as well eg, array/map of records, or record of records.
   * Prerequisite: The top-level schema are of type RECORD only and each fields have default values. ie they are compatible
   * schemas and the generated schema will pick the default value from s1.
   * @param s1 1st input schema
   * @param s2 2nd input schema
   * @return super set schema of s1 and s2
   */
  public static Schema generateSuperSetSchema(Schema s1, Schema s2) {
    if (s1.getType() != s2.getType()) {
      throw new VeniceException("Incompatible schema");
    }

    if (Objects.equals(s1, s2)) {
      return s1;
    }

    // Special handling for String vs Avro string comparision,
    // return the schema with avro.java.string property for string type
    if (s1.getType() == Schema.Type.STRING) {
      return s1.getJsonProps() == null ? s2 : s1;
    }

    switch (s1.getType()) {
      case RECORD:
        if (!StringUtils.equals(s1.getNamespace(), s2.getNamespace())) {
          throw new VeniceException("Trying to merge schema with different namespace.");
        }
        Schema superSetSchema = Schema.createRecord(s1.getName(), s1.getDoc(), s1.getNamespace(), false);
        superSetSchema.setFields(mergeFields(s1, s2));
        return superSetSchema;
      case ARRAY:
        return Schema.createArray(generateSuperSetSchema(s1.getElementType(), s2.getElementType()));
      case MAP:
        return Schema.createMap(generateSuperSetSchema(s1.getValueType(), s2.getValueType()));
      case UNION:
        return unionSchema(s1, s2);
      default:
        throw new VeniceException("Super set schema not supported");
    }
  }

  private static Schema unionSchema(Schema s1, Schema s2) {
    List<Schema> combinedSchema = new ArrayList<>();
    Map<String, Schema> s2Schema = s2.getTypes().stream().collect(Collectors.toMap(s -> s.getName(), s -> s));
    for (Schema s : s1.getTypes()) {
      if (s2Schema.get(s.getName()) != null) {
        combinedSchema.add(generateSuperSetSchema(s, s2Schema.get(s.getName())));
        s2Schema.remove(s.getName());
      } else {
        combinedSchema.add(s);
      }
    }
    s2Schema.forEach((k, v) -> combinedSchema.add(v));

    return Schema.createUnion(combinedSchema);
  }

  private static List<Schema.Field> mergeFields(Schema s1, Schema s2) {
    List<Schema.Field> fields = new ArrayList<>();

    for (Schema.Field f1 : s1.getFields()) {
      Schema.Field f2 = s2.getField(f1.name());
      if (f2 == null) {
        fields.add(new Schema.Field(f1.name(), f1.schema(), f1.doc(), f1.defaultValue(), f1.order()));
      } else {
        fields.add(new Schema.Field(f1.name(), generateSuperSetSchema(f1.schema(), f2.schema()),
            f1.doc() != null ? f1.doc() : f2.doc(), f1.defaultValue()));
      }
    }

    for (Schema.Field f2 : s2.getFields()) {
      if (s1.getField(f2.name()) == null) {
        fields.add(new Schema.Field(f2.name(), f2.schema(), f2.doc(), f2.defaultValue()));
      }
    }
    return fields;
  }

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
    for (Schema s1 : list1) {
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
    if (s1.getType() == Schema.Type.RECORD) {
      for (Schema.Field f1 : s1.getFields()) {
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
}
