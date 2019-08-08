package com.linkedin.venice.schema.avro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

import static org.apache.avro.Schema.Field;
import static org.apache.avro.Schema.Type.*;
import static com.linkedin.venice.schema.avro.WriteComputeSchemaAdapter.WriteComputeOperation.*;

/**
 * A util class that parses arbitrary Avro schema to its' write compute schema.
 *
 * Currently, it supports record partial update and collection merging.
 * See {@link WriteComputeOperation} for details.
 *
 * N.B.
 * 1. We should keep {@link WriteComputeOperation} backward compatible. That's being said, if you change it, make
 * sure to release SN earlier than Samza/H2V plugin.
 *
 * 2. We should ask partners to assign a default value or wrap the field with nullable union if they intent to use
 * "partial put" to create new k/v pair (when the key is not existing in the store).
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
        schema -> new Field("setUnion", (Schema) schema, null, ARRAY_NODE),
        schema -> new Field("setDiff", (Schema) schema, null, ARRAY_NODE)
    }),

    /**
     * Perform map operations on top of the original map. It can be only applied to Avro map.
     * Currently support:
     * 1. mapUnion: add new entries into the original map. It overrides the value if a key has already existed in the map.
     * 2. mapDiff: remove entries from the original array.
     */
    MAP_OPS("MapOps", new Function[] {
        schema -> new Field("mapUnion", (Schema) schema, null, OBJECT_NODE),
        schema -> new Field("mapDiff", Schema.createArray(Schema.create(Schema.Type.STRING)), null, ARRAY_NODE)
    });

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

  private static final ArrayNode ARRAY_NODE = JsonNodeFactory.instance.arrayNode();
  private static final ObjectNode OBJECT_NODE = JsonNodeFactory.instance.objectNode();

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

    return parse(schema, name, null);
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
        return adapter.parseUnion(originSchema, namespace);
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
   * {
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
   * }
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
      String fieldName = null;
      if (field.schema().getType() != RECORD) {
        fieldName = field.name();
      }

      fieldList.add(new Field(field.name(), wrapNoopUnion(recordNamespace, parse(field.schema(),
          fieldName, recordNamespace)), field.doc(), OBJECT_NODE, field.order()));
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

  private Schema parseUnion(Schema unionSchema, String namespace) {
    return createFlattenedUnion(unionSchema.getTypes().stream().sequential()
        .map(type -> parse(type, null, namespace))
        .collect(Collectors.toList()));
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

  private Schema createFlattenedUnion(List<Schema> schemaList) {
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
}