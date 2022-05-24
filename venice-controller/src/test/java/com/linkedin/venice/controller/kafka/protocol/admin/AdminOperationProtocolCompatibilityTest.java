package com.linkedin.venice.controller.kafka.protocol.admin;

import com.linkedin.avro.fastserde.FastSerdeCache;
import com.linkedin.venice.controller.kafka.protocol.serializer.AdminOperationSerializer;
import com.linkedin.venice.schema.avro.SchemaCompatibility;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.linkedin.venice.utils.TestUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AdminOperationProtocolCompatibilityTest {
  @Test
  public void testAdminOperationProtocolCompatibility() throws InterruptedException {
    Map<Integer, Schema> schemaMap = AdminOperationSerializer.initProtocolMap();
    int latestSchemaId = AdminOperationSerializer.LATEST_SCHEMA_ID_FOR_ADMIN_OPERATION;

    Assert.assertNotNull(schemaMap.containsKey(latestSchemaId), "The latest schema should exist!");

    SchemaValidatorBuilder schemaValidatorBuilder = new SchemaValidatorBuilder();
    SchemaValidator schemaValidator = schemaValidatorBuilder.canReadStrategy().validateAll();

    /**
     * Also checked the the schema evolution is acceptable for fast avro.
     */
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      FastSerdeCache fastSerdeCache = new FastSerdeCache(executor);

      Schema latestSchema = schemaMap.get(latestSchemaId);
      schemaMap.forEach((schemaId, schema) -> {
        if (schemaId == latestSchemaId) {
          return;
        }
        SchemaCompatibility.SchemaPairCompatibility backwardCompatibility =
            SchemaCompatibility.checkReaderWriterCompatibility(latestSchema, schema);
        String failMessage = "Older admin operation protocol with schema id: " + schemaId + ", schema: " + schema.toString(true)
            + " is not compatible with the latest admin operation protocol with schema id: " + latestSchemaId
            + ", schema: " + latestSchema.toString(true);
        Assert.assertEquals(backwardCompatibility.getType(), SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
            failMessage);

        /**
         * Validate the compatibility between the latest schema and each historical schema by {@link SchemaValidator} since
         * it could tell whether the schema is a valid schema or not.
         * For example, if we specify a wrong default value: "-1" (string type) to a field with "Long" type, the above
         * compatibility check will still pass with a warning message, but the {@link SchemaValidator} will fail when encountering
         * such kind of invalid schema
         */
        try {
          schemaValidator.validate(latestSchema, Collections.singletonList(schema));
        } catch (SchemaValidationException e) {
          Assert.fail(failMessage);
        } catch (Exception e) {
          Assert.fail("Received schema validation exception, and please check the content of schema with ids: " + latestSchemaId + " or " + schemaId, e);
        }

        /**
         * Validate that fast avro can build a deserializer with an old protocol schema as writer schema and the latest
         * protocol schema as reader schema.
         */
        try {
          fastSerdeCache.buildFastGenericDeserializer(schema, latestSchema);
        } catch (Exception e) {
          Assert.fail("Failed fast avro", e);
        }
      });
    } finally {
      TestUtils.shutdownExecutor(executor);
    }
  }
}
