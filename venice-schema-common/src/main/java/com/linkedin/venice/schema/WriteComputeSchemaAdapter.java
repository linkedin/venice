package com.linkedin.venice.schema;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.Schema;

import static com.linkedin.venice.schema.WriteComputeSchemaAdapter.WriteComputeOperation.*;
import static org.apache.avro.Schema.*;
import static org.apache.avro.Schema.Type.*;


/**
 * A util class that parses arbitrary Avro schema to its' write compute schema.
 *
 * Currently, it supports record partial update, collection merging and deletion
 * See {@link WriteComputeOperation} for details.
 *
 * N.B.
 * 1. We should keep {@link WriteComputeOperation} backward compatible. That's being said, if you change it, make
 * sure to release SN earlier than Samza/H2V plugin.
 *
 * 2. We should ask partners to assign a default value or wrap the field with nullable union if they intent to use
 * "partial put" to create new k/v pair (when the key is not existing in the store).
 *
 * 3. nested parsing is not allowed in order to reduce the complexity. Only the top level fields will be parsed.
 * Nested Record/Array/Map will remain the same after parsing.
 */
public class WriteComputeSchemaAdapter {

  /**
   * This enum describe the possible write compute operations Venice supports
   */
  public enum WriteComputeOperation {
    /**
     * Mark to ignore the field. It's used for "partial put" and can be applied to any kind of schema.
     * It's also the default for all record fields in the write compute schema.
     */
    NO_OP("NoOp"),

    /**
     * Perform list operations on top of the original array. It can be only applied to Avro array.
     * Currently support:
     * 1. setUnion: add elements into the original array, as if it was a sorted set. (e.g.: duplicates will be pruned.)
     * 2. setDiff: remove elements from the original array, as if it was a sorted set.
     */
    LIST_OPS("ListOps", new Function[] {
        schema -> AvroCompatibilityHelper.createSchemaField(SET_UNION, (Schema) schema, null, Collections.emptyList()),
        schema -> AvroCompatibilityHelper.createSchemaField(SET_DIFF, (Schema) schema, null, Collections.emptyList())
    }),

    /**
     * Perform map operations on top of the original map. It can be only applied to Avro map.
     * Currently support:
     * 1. mapUnion: add new entries into the original map. It overrides the value if a key has already existed in the map.
     * 2. mapDiff: remove entries from the original array.
     */
    MAP_OPS("MapOps", new Function[] {
        schema -> AvroCompatibilityHelper.createSchemaField(MAP_UNION, (Schema) schema, null, Collections.emptyMap()),
        schema -> AvroCompatibilityHelper.createSchemaField(MAP_DIFF, Schema.createArray(Schema.create(Schema.Type.STRING)), null, Collections.emptyList())
    }),

    /**
     * Marked to remove a record completely. This is used when returning writeComputeSchema with
     * a RECORD type. The returned schema is a union of the record type and a delete operation
     * record.
     *
     * Note: This is only used for non-nested records (it's only intended for removing the whole
     * record. Removing fields inside of a record is not supported.
     */
    DEL_OP("DelOp");

    //a name that meets class naming convention
    final String name;

    final Optional<Function<Schema, Schema.Field>[]> params;

    WriteComputeOperation(String name) {
      this.name = name;
      this.params = Optional.empty();
    }

    WriteComputeOperation(String name, Function<Schema, Schema.Field>[] params) {
      this.name = name;
      this.params = Optional.of(params);
    }

    String getName() {
      return name;
    }

    String getUpperCamelName() {
      if (name.isEmpty()) {
        return  name;
      }

      return name.substring(0, 1).toUpperCase() + name.substring(1);
    }
  }

  //Instantiate some constants here so that they could be reused.
  private static final String WRITE_COMPUTE_RECORD_SCHEMA_SUFFIX = "WriteOpRecord";

  //List operations
  public static final String SET_UNION = "setUnion";
  public static final String SET_DIFF = "setDiff";

  //Map operations
  public static final String MAP_UNION = "mapUnion";
  public static final String MAP_DIFF = "mapDiff";

  private WriteComputeSchemaAdapter() {}

  public static Schema parse(String schemaStr) {
    return parse(Schema.parse(schemaStr));
  }

  public static Schema parse(Schema schema) {
    String name = null;

    /*if this is a record, we'd like to append a suffix to the name so that the write
    schema name wouldn't collide with the original schema name
    */
    if (schema.getType() == RECORD) {
      name = schema.getName() + WRITE_COMPUTE_RECORD_SCHEMA_SUFFIX;
    }

    WriteComputeSchemaAdapter adapter = new WriteComputeSchemaAdapter();
    return adapter.wrapDelOpUnion(parse(schema, name, null));
  }

  /**
   * Parse the given schema to its corresponding write compute schema
   * @param derivedSchemaName the name of the output derived schema. This can be null and in that case, it will
   *                          inherit the same name from the original schema.
   * @param namespace This can be null and it is only set up for arrays/maps in a record. Since the write compute
   *                  operation record will be called "ListOps"/"MapOps" regardless of the element type, duplicate
   *                  definition error might occur when a Record contains multiple arrays/maps. In case it happens,
   *                  we inherit field name as the namespace to distinguish them.
   */
  private static Schema parse(Schema originSchema, String derivedSchemaName, String namespace) {
    WriteComputeSchemaAdapter adapter = new WriteComputeSchemaAdapter();

    switch (originSchema.getType()) {
      case RECORD:
        return adapter.parseRecord(originSchema, derivedSchemaName);
      case ARRAY:
        return adapter.parseArray(originSchema, derivedSchemaName, namespace);
      case MAP:
        return adapter.parseMap(originSchema, derivedSchemaName, namespace);
      case UNION:
        return adapter.parseUnion(originSchema, derivedSchemaName, namespace);
      default:
        return originSchema;
    }
  }

  /**
   * Wrap a record schema with possible write compute operations. Recursive parsing happens for each field.
   * e.g.
   * origin record schema:
   * {
   *   "type" : "record",
   *   "name" : "testRecord",
   *   "fields" : [ {
   *     "name" : "intField",
   *     "type" : "int",
   *     "default" : 0
   *   }, {
   *     "name" : "floatArray",
   *     "type" : {
   *       "type" : "array",
   *       "items" : "float"
   *     },
   *     "default" : [ ]
   *   } ]
   * }
   *
   * write compute record schema:
   * [{
   *   "type" : "record",
   *   "name" : "testRecordWriteOpRecord",
   *   "fields" : [ {
   *     "name" : "intField",
   *     "type" : [ {
   *       "type" : "record",
   *       "name" : "NoOp",
   *       "fields" : [ ]
   *     }, "int" ],
   *     "default" : { }
   *   }, {
   *     "name" : "floatArray",
   *     "type" : [ "NoOp", {
   *       "type" : "record",
   *       "name" : "floatArrayListOps",
   *       "fields" : [ {
   *         "name" : "setUnion",
   *         "type" : {
   *           "type" : "array",
   *           "items" : "float"
   *         },
   *         "default" : [ ]
   *       }, {
   *         "name" : "setDiff",
   *         "type" : {
   *           "type" : "array",
   *           "items" : "float"
   *         },
   *         "default" : [ ]
   *       } ]
   *     }, {
   *       "type" : "array",
   *       "items" : "float"
   *     } ],
   *     "default" : { }
   *   } ]
   * }, {
   *   "type" : "record",
   *   "name" : "DelOp",
   *   "fields" : [ ]
   * } ]
   *
   * @param recordSchema the original record schema
   */
  private Schema parseRecord(Schema recordSchema, String derivedSchemaName) {
    String recordNamespace = recordSchema.getNamespace();

    if (derivedSchemaName == null) {
      derivedSchemaName = recordSchema.getName();
    }

    Schema newSchema = Schema.createRecord(derivedSchemaName, recordSchema.getDoc(), recordNamespace,
        recordSchema.isError());
    List<Field> fieldList = new ArrayList<>();
    for (Field field : recordSchema.getFields()) {
      if (!AvroCompatibilityHelper.fieldHasDefault(field)) {
        throw new VeniceException(String.format("Cannot generate derived schema because field: \"%s\" "
            + "does not have a default value.", field.name()));
      }

      //parse each field. We'd like to skip parsing "RECORD" type in order to avoid recursive parsing
      fieldList.add(AvroCompatibilityHelper.createSchemaField(field.name(), wrapNoopUnion(recordNamespace, field.schema().getType() == RECORD ?
          field.schema() : parse(field.schema(), field.name(), recordNamespace)), field.doc(), Collections.emptyMap(),
          field.order()));
    }

    newSchema.setFields(fieldList);

    return newSchema;
  }

  /**
   * Wrap an array schema with possible write compute array operations.
   * N. B. We're not supporting nested operation such as adding elements to the inner array for an
   * array of array. Nested operations increase the complexity on both schema generation side and
   * write compute process side. We'll add the support in the future if it's needed.
   *
   * e.g.
   * origin array schema:
   * { "type": "array", "items": "int" }
   *
   * write compute array schema:
   * [ {
   *   "type" : "record",
   *   "name" : "ListOps",
   *   "fields" : [ {
   *     "name" : "setUnion",
   *     "type" : {
   *       "type" : "array",
   *       "items" : "int"
   *     },
   *     "default" : [ ]
   *   }, {
   *     "name" : "setDiff",
   *     "type" : {
   *       "type" : "array",
   *       "items" : "int"
   *     },
   *     "default" : [ ]
   *   } ]
   * }, {
   *   "type" : "array",
   *   "items" : "int"
   * } ]
   *
   * @param arraySchema the original array schema
   * @param name
   * @param namespace The namespace in "ListOps" record. See {@link #parse(Schema, String, String)} for details.
   */
  private Schema parseArray(Schema arraySchema, String name, String namespace) {
    return Schema.createUnion(Arrays.asList(getCollectionOperation(LIST_OPS, arraySchema, name, namespace),
        arraySchema));
  }

  /**
   * Wrap up a map schema with possible write compute map operations.
   * e.g.
   * origin map schema:
   * { "type": "map", "values": "int"}
   *
   * write compute map schema
   * [ {
   *   "type" : "record",
   *   "name" : "MapOps",
   *   "fields" : [ {
   *     "name" : "mapUnion",
   *     "type" : {
   *       "type" : "map",
   *       "values" : "int"
   *     },
   *     "default" : { }
   *   }, {
   *     "name" : "mapDiff",
   *     "type" : {
   *       "type" : "array",
   *       "items" : "string"
   *     },
   *     "default" : [ ]
   *   } ]
   * }, {
   *   "type" : "map",
   *   "values" : "int"
   * } ]
   *
   * @param mapSchema the original map schema
   * @param namespace the namespace in "MapOps" record. See {@link #parse(Schema, String, String)} for details.
   */
  private Schema parseMap(Schema mapSchema, String name, String namespace) {
    return Schema.createUnion(Arrays.asList(getCollectionOperation(MAP_OPS, mapSchema, name, namespace),
        mapSchema));
  }

  /**
   * Wrap up a union schema with possible write compute operations.
   *
   * N.B.: if it's an top level union field, the parse will try to parse the elements(except for RECORD)
   * inside the union. It's not supported if an union contains more than 1 collections since it will
   * confuse the interpreter about which elements the operation is supposed to be applied.
   *
   * e.g.
   * original schema:
   *{
   *   "type" : "record",
   *   "name" : "ecord",
   *   "fields" : [ {
   *     "name" : "nullableArrayField",
   *     "type" : [ "null", {
   *       "type" : "array",
   *       "items" : {
   *         "type" : "record",
   *         "name" : "simpleRecord",
   *         "fields" : [ {
   *           "name" : "intField",
   *           "type" : "int",
   *           "default" : 0
   *         } ]
   *       }
   *     } ],
   *     "default" : null
   *   } ]
   * }
   *
   * write compute schema:
   * [ {
   *   "type" : "record",
   *   "name" : "testRecordWriteOpRecord",
   *   "fields" : [ {
   *     "name" : "nullableArrayField",
   *     "type" : [ {
   *       "type" : "record",
   *       "name" : "NoOp",
   *       "fields" : [ ]
   *     }, "null", {
   *       "type" : "record",
   *       "name" : "nullableArrayFieldListOps",
   *       "fields" : [ {
   *         "name" : "setUnion",
   *         "type" : {
   *           "type" : "array",
   *           "items" : {
   *             "type" : "record",
   *             "name" : "simpleRecord",
   *             "fields" : [ {
   *               "name" : "intField",
   *               "type" : "int",
   *               "default" : 0
   *             } ]
   *           }
   *         },
   *         "default" : [ ]
   *       }, {
   *         "name" : "setDiff",
   *         "type" : {
   *           "type" : "array",
   *           "items" : "simpleRecord"
   *         },
   *         "default" : [ ]
   *       } ]
   *     }, {
   *       "type" : "array",
   *       "items" : "simpleRecord"
   *     } ],
   *     "default" : { }
   *   } ]
   * }, {
   *   "type" : "record",
   *   "name" : "DelOp",
   *   "fields" : [ ]
   * } ]
   */
  private Schema parseUnion(Schema unionSchema, String name, String namespace) {
    containsOnlyOneCollection(unionSchema);
    return createFlattenedUnion(unionSchema.getTypes().stream().sequential()
        .map(schema ->{
          Schema.Type type = schema.getType();
          if (type == RECORD) {
            return schema;
          }

          return parse(schema, name, namespace);
        })
        .collect(Collectors.toList()));
  }

  /**
   * Utility function that checks to make sure that given a union schema, there only exists 1 collection type amongst the
   * provided types.  Multiple collections will make the result of the flattened write compute schema lead to ambiguous behavior
   *
   * @param unionSchema a union schema to validate.
   * @throws VeniceException When the unionSchema contains more then one collection type
   */
  private void containsOnlyOneCollection(Schema unionSchema) {
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
   * Wrap up one or more schema with Noop record into a union. If the origin schema is an union,
   * it will be flattened. (instead of becoming nested unions)
   * @param schemaList
   * @return an union schema that contains all schemas in the list plus Noop record
   */
  private Schema wrapNoopUnion(String namespace, Schema... schemaList) {
    LinkedList<Schema> list = new LinkedList<>(Arrays.asList(schemaList));
    //always put NO_OP at the first place so that it will be the default value of the union
    list.addFirst(getNoOpOperation(namespace));

    return createFlattenedUnion(list);
  }

  /**
   * Wrap up schema with DelOp record into a union. The origin schema
   * must be a record schema
   * @param schema
   * @return an union schema that contains all schemas in the list plus Noop record
   */
  private Schema wrapDelOpUnion(Schema schema) {
    if (schema.getType() != RECORD) {
      return schema;
    }

    LinkedList<Schema> list = new LinkedList<>();
    list.add(schema);
    list.add(getDelOpOperation(schema.getNamespace()));

    return createFlattenedUnion(list);
  }



  public static Schema createFlattenedUnion(List<Schema> schemaList) {
    List<Schema> flattenedSchemaList = new ArrayList<>();
    for (Schema schema : schemaList) {
      //if the origin schema is union, we'd like to flatten it
      //we don't need to do it recursively because Avro doesn't support nested union
      if (schema.getType() == UNION) {
        flattenedSchemaList.addAll(schema.getTypes());
      } else {
        flattenedSchemaList.add(schema);
      }
    }

    return Schema.createUnion(flattenedSchemaList);
  }

  private Schema getCollectionOperation(WriteComputeOperation collectionOperation, Schema collectionSchema, String name,
      String namespace) {
    if (name == null) {
      name = collectionOperation.getName();
    } else {
      name = name + collectionOperation.getUpperCamelName();
    }

    Schema operationSchema = Schema.createRecord(name, null, namespace, false);
    operationSchema.setFields(Arrays.stream(collectionOperation.params.get())
        .map(param -> param.apply(collectionSchema))
        .collect(Collectors.toList()));
    return operationSchema;
  }

  public Schema getNoOpOperation(String namespace) {
    Schema noOpSchema = Schema.createRecord(NO_OP.getName(), null, namespace, false);

    //Avro requires every record to have a list of fields even if it's empty... Otherwise, NPE
    //will be thrown out during parsing the schema.
    noOpSchema.setFields(Collections.EMPTY_LIST);

    return noOpSchema;
  }

  public Schema getDelOpOperation(String namespace) {
    Schema delOpSchema = Schema.createRecord(DEL_OP.getName(), null, namespace, false);

    //Avro requires every record to have a list of fields even if it's empty... Otherwise, NPE
    //will be thrown out during parsing the schema.
    delOpSchema.setFields(Collections.EMPTY_LIST);

    return delOpSchema;
  }
}