package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_RECORD_V2_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.NAME_WITH_DETAILS_V1_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.STRING_SCHEMA;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1OHSupersetSchemaWithNullFirstName;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV1Schema;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToNameRecordV2Schema;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToNameWithDetailsV1OHSupersetSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TARGET_WRITER_VALUE_SCHEMA_ID_PROP;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.hadoop.exceptions.VeniceSchemaMismatchException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * End-to-end coverage for input value-schema projection in VPJ: input data whose value schema is a strict superset of a
 * registered writer (target) value schema is projected down to the writer schema (via {@code VeniceSchemaProjector})
 * before serialization. Enabled by setting {@code target.writer.value.schema.id} to a positive id.
 */
public class TestPushJobWithValueSchemaProjection {
  private static final int TEST_TIMEOUT = 90 * Time.MS_PER_SECOND;

  private VeniceClusterWrapper veniceCluster;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    VeniceClusterCreateOptions options =
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1).numberOfServers(1).numberOfRouters(1).build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    IOUtils.closeQuietly(veniceCluster);
  }

  /**
   * Happy path: store is registered with the writer schema (V1); input data uses a strict superset schema (V2, which
   * adds {@code age}). With the target writer schema id supplied, each record is projected down
   * to V1 and the push completes. Reading the data back returns V1 records with the {@code age} field dropped.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testProjectsSupersetInputToWriterSchemaEndToEnd() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    // Input data file uses the V2 (superset) value schema.
    writeSimpleAvroFileWithStringToNameRecordV2Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("schema-projection-store");

    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    int writerSchemaId;
    try (ControllerClient controllerClient =
        createStoreForJob(veniceCluster, STRING_SCHEMA.toString(), NAME_RECORD_V1_SCHEMA.toString(), props)) {
      // The store's single registered value schema (V1) is the projection target.
      SchemaResponse writerSchemaIdResponse =
          controllerClient.getValueSchemaID(storeName, NAME_RECORD_V1_SCHEMA.toString());
      Assert.assertFalse(writerSchemaIdResponse.isError(), writerSchemaIdResponse.getError());
      writerSchemaId = writerSchemaIdResponse.getId();
      Assert.assertTrue(writerSchemaId > 0);
    }

    props.setProperty(TARGET_WRITER_VALUE_SCHEMA_ID_PROP, Integer.toString(writerSchemaId));

    try (VenicePushJob job = new VenicePushJob("test-push-projection-happy-path", props)) {
      job.run();
    }

    try (AvroGenericStoreClient<String, GenericRecord> avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, true, true, () -> {
        for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; i++) {
          GenericRecord value = avroClient.get(Integer.toString(i)).get();
          Assert.assertNotNull(value, "Missing value for key: " + i);
          Assert.assertEquals(value.get("firstName").toString(), "first_name_" + i);
          Assert.assertEquals(value.get("lastName").toString(), "last_name_" + i);
          // The value was projected to the writer (V1) schema, which has no "age" field.
          Assert.assertNull(value.getSchema().getField("age"), "Projected record should not retain the 'age' field");
        }
      });
    }
  }

  /**
   * Recursive OH evolution end-to-end: the writer schema has a nested {@code address} record and a {@code contacts}
   * array of records; the input superset wraps top-level fields as [null, X], appends a top-level {@code age}, and adds
   * extra nullable fields inside the nested record ({@code zip}) and the array element record ({@code primary}).
   * Projection must recurse through the record, the array elements, unwrap the nullable wrappers, and drop every extra
   * field at every level. Reading back yields clean writer-schema records.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testProjectsNestedAndArrayOHEvolutionEndToEnd() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    writeSimpleAvroFileWithStringToNameWithDetailsV1OHSupersetSchema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("schema-projection-nested-store");

    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    int writerSchemaId;
    try (ControllerClient controllerClient =
        createStoreForJob(veniceCluster, STRING_SCHEMA.toString(), NAME_WITH_DETAILS_V1_SCHEMA.toString(), props)) {
      SchemaResponse writerSchemaIdResponse =
          controllerClient.getValueSchemaID(storeName, NAME_WITH_DETAILS_V1_SCHEMA.toString());
      Assert.assertFalse(writerSchemaIdResponse.isError(), writerSchemaIdResponse.getError());
      writerSchemaId = writerSchemaIdResponse.getId();
      Assert.assertTrue(writerSchemaId > 0);
    }

    props.setProperty(TARGET_WRITER_VALUE_SCHEMA_ID_PROP, Integer.toString(writerSchemaId));

    try (VenicePushJob job = new VenicePushJob("test-push-projection-nested", props)) {
      job.run();
    }

    try (AvroGenericStoreClient<String, GenericRecord> avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(TEST_TIMEOUT, TimeUnit.MILLISECONDS, true, true, () -> {
        for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; i++) {
          GenericRecord value = avroClient.get(Integer.toString(i)).get();
          Assert.assertNotNull(value, "Missing value for key: " + i);
          // Top-level nullable wrappers unwrapped; extra top-level "age" dropped.
          Assert.assertEquals(value.get("firstName").toString(), "first_name_" + i);
          Assert.assertEquals(value.get("lastName").toString(), "last_name_" + i);
          Assert.assertNull(value.getSchema().getField("age"), "extra top-level field must be dropped");
          // Nested record projected: city retained, extra "zip" dropped.
          GenericRecord address = (GenericRecord) value.get("address");
          Assert.assertEquals(address.get("city").toString(), "city_" + i);
          Assert.assertNull(address.getSchema().getField("zip"), "extra nested field must be dropped");
          // Array element record projected: kind retained, extra "primary" dropped.
          @SuppressWarnings("unchecked")
          List<GenericRecord> contacts = (List<GenericRecord>) value.get("contacts");
          Assert.assertEquals(contacts.size(), 1);
          Assert.assertEquals(contacts.get(0).get("kind").toString(), "kind_" + i);
          Assert
              .assertNull(contacts.get(0).getSchema().getField("primary"), "extra array-element field must be dropped");
        }
      });
    }
  }

  /**
   * Null value into a non-nullable writer field: the input wraps {@code firstName} as {@code [null, string]} and a
   * record actually carries null, while the writer keeps {@code firstName} non-nullable. Schema-level projection
   * validation passes (the nullability drift is accepted), so the push proceeds; the null is copied through during
   * projection and then fails when the record is serialized against the (non-nullable) writer schema. The push must
   * therefore fail rather than silently write bad data.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testProjectionFailsWhenNullableInputValueIsNullForNonNullableWriterField() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    // Input data uses the OH-style superset of V1, but firstName (nullable in the input) is null.
    writeSimpleAvroFileWithStringToNameRecordV1OHSupersetSchemaWithNullFirstName(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("schema-projection-null-value-store");

    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    int writerSchemaId;
    try (ControllerClient controllerClient =
        createStoreForJob(veniceCluster, STRING_SCHEMA.toString(), NAME_RECORD_V1_SCHEMA.toString(), props)) {
      SchemaResponse writerSchemaIdResponse =
          controllerClient.getValueSchemaID(storeName, NAME_RECORD_V1_SCHEMA.toString());
      Assert.assertFalse(writerSchemaIdResponse.isError(), writerSchemaIdResponse.getError());
      writerSchemaId = writerSchemaIdResponse.getId();
      Assert.assertTrue(writerSchemaId > 0);
    }

    props.setProperty(TARGET_WRITER_VALUE_SCHEMA_ID_PROP, Integer.toString(writerSchemaId));
    try (VenicePushJob job = new VenicePushJob("test-push-projection-null-value", props)) {
      // The job must fail; the null firstName cannot be serialized against the non-nullable writer schema.
      Assert.expectThrows(VeniceException.class, job::run);
    }
  }

  /**
   * The supplied target writer value schema id does not exist in the store: projection cannot be configured and the
   * push fails fast with a clear error.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testProjectionFailsWhenTargetWriterSchemaIdNotFound() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    writeSimpleAvroFileWithStringToNameRecordV2Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("schema-projection-missing-id-store");

    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    createStoreForJob(veniceCluster, STRING_SCHEMA.toString(), NAME_RECORD_V1_SCHEMA.toString(), props).close();

    int nonExistentWriterSchemaId = 9999;
    props.setProperty(TARGET_WRITER_VALUE_SCHEMA_ID_PROP, Integer.toString(nonExistentWriterSchemaId));

    try (VenicePushJob job = new VenicePushJob("test-push-projection-missing-id", props)) {
      VeniceException exception = Assert.expectThrows(VeniceException.class, job::run);
      Assert.assertTrue(
          exception.getMessage()
              .contains("Failed to fetch target writer value schema id: " + nonExistentWriterSchemaId),
          "Unexpected exception message: " + exception.getMessage());
    }
  }

  /**
   * The input value schema is not a strict superset of the target writer value schema (input is V1, writer is V2 which
   * additionally requires {@code age}): the superset preflight check rejects the push.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testProjectionFailsWhenInputNotSupersetOfWriterSchema() throws Exception {
    File inputDir = Utils.getTempDataDirectory();
    // Input data file uses the V1 schema, which is NOT a superset of the V2 writer schema.
    writeSimpleAvroFileWithStringToNameRecordV1Schema(inputDir);
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    String storeName = Utils.getUniqueString("schema-projection-not-superset-store");

    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    int writerSchemaId;
    try (ControllerClient controllerClient =
        createStoreForJob(veniceCluster, STRING_SCHEMA.toString(), NAME_RECORD_V2_SCHEMA.toString(), props)) {
      SchemaResponse writerSchemaIdResponse =
          controllerClient.getValueSchemaID(storeName, NAME_RECORD_V2_SCHEMA.toString());
      Assert.assertFalse(writerSchemaIdResponse.isError(), writerSchemaIdResponse.getError());
      writerSchemaId = writerSchemaIdResponse.getId();
    }

    props.setProperty(TARGET_WRITER_VALUE_SCHEMA_ID_PROP, Integer.toString(writerSchemaId));

    try (VenicePushJob job = new VenicePushJob("test-push-projection-not-superset", props)) {
      VeniceSchemaMismatchException exception = Assert.expectThrows(VeniceSchemaMismatchException.class, job::run);
      Assert.assertTrue(
          exception.getMessage().contains("not a superset of the target writer value schema"),
          "Unexpected exception message: " + exception.getMessage());
    }
  }
}
