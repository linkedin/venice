package com.linkedin.venice.samza;

import static com.linkedin.venice.utils.IntegrationTestPushUtils.getSamzaProducerConfig;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.samza.SamzaException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class VeniceSystemFactoryTest {
  private static final int TEST_TIMEOUT = 15000; // ms

  private VeniceClusterWrapper cluster;

  @BeforeClass
  public void setUp() {
    cluster = ServiceFactory.getVeniceCluster();
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  /**
   * Write a record using the Samza SystemProducer for Venice, then verify we can read that record.
   */
  @Test // (timeOut = TEST_TIMEOUT * 2)
  public void testGetProducer() throws Exception {
    String keySchema = "\"string\"";
    String valueSchema = "{\n" + "  \"type\" : \"record\",\n" + "  \"name\" : \"testRecord\",\n"
        + "  \"fields\" : [ {\n" + "    \"name\" : \"number\",\n" + "    \"type\" : [ \"double\", \"null\" ],\n"
        + "    \"default\" : 100.0\n" + "  }, {\n" + "    \"name\" : \"string\",\n"
        + "    \"type\" : [ \"string\", \"null\" ],\n" + "    \"default\" : \"100\"\n" + "  }, {\n"
        + "    \"name\" : \"intArray\",\n" + "    \"type\" : {\n" + "      \"type\" : \"array\",\n"
        + "      \"items\" : \"int\"\n" + "    },\n" + "    \"default\" :  [ ]\n" + "  } ]\n" + "}";

    String storeName = Utils.getUniqueString("store");
    Schema writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchemaStr(valueSchema);

    cluster.useControllerClient(controllerClient -> {
      TestUtils.assertCommand(controllerClient.createNewStore(storeName, "owner", keySchema, valueSchema));
      // Enable hybrid and write-compute
      TestUtils.assertCommand(
          controllerClient.updateStore(
              storeName,
              new UpdateStoreQueryParams().setHybridRewindSeconds(10).setHybridOffsetLagThreshold(10)));

      // Generate write compute schema
      TestUtils.assertCommand(controllerClient.addDerivedSchema(storeName, 1, writeComputeSchema.toString()));
    });
    String key = "keystring";

    Schema valueRecordSchema = Schema.parse(valueSchema);
    Schema intArraySchema = valueRecordSchema.getField("intArray").schema();
    GenericRecord batchValue = new GenericData.Record(Schema.parse(valueSchema));
    batchValue.put("string", null);
    batchValue.put("number", 0.0);
    batchValue.put("intArray", Collections.emptyList());

    cluster.createVersion(storeName, keySchema, valueSchema, Stream.of(new AbstractMap.SimpleEntry(key, batchValue)));

    ClientConfig config = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL());

    SystemProducer producer = null;

    try (AvroGenericStoreClient<String, GenericRecord> client = ClientFactory.getAndStartGenericAvroClient(config)) {
      producer = IntegrationTestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM);

      // Send the record to Venice using the SystemProducer
      GenericRecord record = new GenericData.Record(Schema.parse(valueSchema));
      record.put("string", "somestring");
      record.put("number", 3.74);
      record.put("intArray", new GenericData.Array<>(intArraySchema, Collections.singletonList(1)));

      IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, key, record);

      // Verify we got the right record
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, true, () -> {
        GenericRecord recordFromVenice;
        try {
          recordFromVenice = client.get(key).get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        assertNotNull(
            recordFromVenice,
            "Value for key: '" + key + "' should not be null. This means not even the batch data made it in!");

        Object stringField = recordFromVenice.get("string");
        assertNotNull(stringField, "'string' field should not be null. This means the RT data did not make it in.");
        assertEquals(stringField.toString(), "somestring");

        Object numberField = recordFromVenice.get("number");
        assertNotNull(numberField, "'number' field should not be null");
        assertEquals(numberField, 3.74);

        Object intArrayField = recordFromVenice.get("intArray");
        assertEquals(intArrayField, new GenericData.Array<>(intArraySchema, Collections.singletonList(1)));
      });

      // Delete the record
      IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, key, null);

      // Verify the delete
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        GenericRecord deletedRecord = null;
        try {
          deletedRecord = client.get(key).get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        assertNull(deletedRecord);
      });

    } finally {
      if (producer != null) {
        producer.stop();
      }
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSchemaMismatchError() throws Exception {
    String storeName = cluster.createStore(0);
    SystemProducer producer = IntegrationTestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.BATCH);
    try {
      // Send a record with a wrong schema, this is byte[] of chars "1", "2", "3", expects int
      assertThrows(
          SamzaException.class,
          () -> IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, new byte[] { 49, 50, 51 }, 0));
    } finally {
      producer.stop();
    }
  }

  @DataProvider(name = "testSerializationParams")
  public static Object[][] testSerializationParams() {
    return new Object[][] { { 5, 10, "\"int\"" }, { new Utf8("one"), new Utf8("two"), "\"string\"" },
        { 6L, 8L, "\"long\"" }, { 9.12D, 12.45D, "\"double\"" }, { 1.6F, 7.4F, "\"float\"" },
        { ByteBuffer.wrap(new byte[] { 0x1, 0x2, 0x3 }), ByteBuffer.wrap(new byte[] { 0xb, 0xc, 0xd }), "\"bytes\"" },
        { true, false, "\"boolean\"" }, };
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "testSerializationParams")
  public void testSerialization(Object key, Object value, String schema) throws Exception {
    testSerializationCast(key, key, value, value, schema);
  }

  @DataProvider(name = "testSerializationCastParams")
  public static Object[][] testSerializationCastParams() {
    String complexSchema = "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"SomeRecord\",\n" + "  \"fields\": [\n"
        + "     {\"name\": \"int_field\", \"type\": \"int\"}\n" + "   ]\n" + "}";
    String complexSchemaWithExtraProperty =
        "{\n" + "  \"type\": \"record\",\n" + "  \"name\": \"SomeRecord\",\n" + "  \"fields\": [\n"
            + "     {\"name\": \"int_field\", \"type\": \"int\", \"java\": \"abc\"}\n" + "   ]\n" + "}";
    GenericRecord complexKeyRecord = new GenericData.Record(Schema.parse(complexSchemaWithExtraProperty));
    complexKeyRecord.put("int_field", 100);
    // Value record comparison will compare the associated schema by default, so we will use the schema without extra
    // property here.
    GenericRecord complexValueRecord = new GenericData.Record(Schema.parse(complexSchema));
    complexValueRecord.put("int_field", 200);
    return new Object[][] {
        { new byte[] { 0x3, 0x4, 0x5 }, ByteBuffer.wrap(new byte[] { 0x3, 0x4, 0x5 }), new byte[] { 0xd, 0xe, 0xf },
            ByteBuffer.wrap(new byte[] { 0xd, 0xe, 0xf }), "\"bytes\"" },
        { "three", "three", "four", new Utf8("four"), "\"string\"" },
        { complexKeyRecord, complexKeyRecord, complexValueRecord, complexValueRecord, complexSchema }, };
  }

  /**
   * Avro sometimes returns a different type when deserializing.  For example, a serialized String is deserialized
   * into a Utf8 object.  This method can be used to get around the resulting assertion error when the returned object
   * is equivalent but of a different class.
   */
  @Test(timeOut = TEST_TIMEOUT, dataProvider = "testSerializationCastParams")
  public void testSerializationCast(Object writeKey, Object readKey, Object value, Object expectedValue, String schema)
      throws Exception {
    String storeName = Utils.getUniqueString("schema-test-store");

    cluster.useControllerClient(client -> {
      client.createNewStore(storeName, "owner", schema, schema);

      SystemProducer producer = IntegrationTestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.BATCH);
      try {
        IntegrationTestPushUtils.sendStreamingRecord(producer, storeName, writeKey, value);
      } finally {
        producer.stop();
      }

      client.writeEndOfPush(storeName, 1);
      cluster.waitVersion(storeName, 1, client);
    });

    ClientConfig config = ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL());

    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(config)) {
      Object actualValue = client.get(readKey).get();
      assertEquals(
          actualValue,
          expectedValue,
          "Expected [" + expectedValue + "] of type " + expectedValue.getClass() + " but found [" + actualValue
              + "] of type " + actualValue.getClass());
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testGetProducerRunningFabric() throws Exception {
    VeniceSystemFactory factory = new VeniceSystemFactory();
    Map<String, String> samzaConfig =
        getSamzaProducerConfig(cluster, "test-store-sr", Version.PushType.STREAM_REPROCESSING);

    // null runningFabric
    SystemProducer producer1 = factory.getProducer("venice", new MapConfig(samzaConfig), null);
    if (producer1 instanceof VeniceSystemProducer) {
      assertEquals(((VeniceSystemProducer) producer1).getRunningFabric(), null);
    }

    // set runningFabric through system Config
    System.setProperty("com.linkedin.app.env", "dc-1");
    SystemProducer producer2 = factory.getProducer("venice", new MapConfig(samzaConfig), null);
    if (producer2 instanceof VeniceSystemProducer) {
      assertEquals(((VeniceSystemProducer) producer2).getRunningFabric(), "dc-1");
    }

    // set runningFabric through samza Config.
    samzaConfig.put("com.linkedin.app.env", "dc-0");
    SystemProducer producer3 = factory.getProducer("venice", new MapConfig(samzaConfig), null);
    if (producer3 instanceof VeniceSystemProducer) {
      assertEquals(((VeniceSystemProducer) producer3).getRunningFabric(), "dc-0");
    }

    // set runningFabric to parent fabric.
    samzaConfig.put("com.linkedin.app.env", "dc-parent");
    SystemProducer producer4 = factory.getProducer("venice", new MapConfig(samzaConfig), null);
    if (producer4 instanceof VeniceSystemProducer) {
      assertEquals(((VeniceSystemProducer) producer4).getRunningFabric(), "dc-parent");
    }
  }
}
