package com.linkedin.venice.samza;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.schema.WriteComputeSchemaAdapter;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.TestPushUtils;
import com.linkedin.venice.utils.TestUtils;

import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.system.SystemProducer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.linkedin.venice.schema.WriteComputeSchemaAdapter.*;
import static org.testng.Assert.*;


public class VeniceSystemFactoryTest {
  private static final int TEST_TIMEOUT = 15000; // ms

  private VeniceClusterWrapper cluster;

  @BeforeClass
  private void setUp() {
    cluster = ServiceFactory.getVeniceCluster();
  }

  @AfterClass
  private void tearDown() {
    IOUtils.closeQuietly(cluster);
  }

  @DataProvider(name = "testGetProducerParams")
  public static Object[][] testGetProducerParams() {
    return new Object[][] {
        { new Pair<>(VeniceSystemFactory.VENICE_PARTITIONERS, null) },
        { new Pair<>(VeniceSystemFactory.VENICE_PARTITIONERS, DefaultVenicePartitioner.class.getName()) }
    };
  }
  /**
   * Write a record using the Samza SystemProducer for Venice, then verify we can read that record.
   */
  @Test(timeOut = TEST_TIMEOUT, dataProvider = "testGetProducerParams")
  public void testGetProducer(Pair<String, String>... optionalConfigs) throws Exception {
    String keySchema = "\"string\"";
    String valueSchema =
        "{\n" +
        "  \"type\" : \"record\",\n" +
        "  \"name\" : \"testRecord\",\n" +
        "  \"fields\" : [ {\n" +
        "    \"name\" : \"number\",\n" +
        "    \"type\" : [ \"double\", \"null\" ],\n" +
        "    \"default\" : 100\n" + "  }, {\n" +
        "    \"name\" : \"string\",\n" +
        "    \"type\" : [ \"string\", \"null\" ],\n" +
        "    \"default\" : 100\n" + "  }, {\n" +
        "    \"name\" : \"intArray\",\n" +
        "    \"type\" : {\n" +
        "      \"type\" : \"array\",\n" +
        "      \"items\" : \"int\"\n" +
        "    },\n" +
        "    \"default\" :  [ ]\n" +
        "  } ]\n" +
        "}";
    String storeName = TestUtils.getUniqueString("store");
    Schema writeComputeSchema = WriteComputeSchemaAdapter.parse(valueSchema);

    try (ControllerClient client = cluster.getControllerClient()) {
      client.createNewStore(storeName, "owner", keySchema, valueSchema);
      // Generate write compute schema
      client.addDerivedSchema(storeName, 1, writeComputeSchema.toString());
      // Enable hybrid
      client.updateStore(storeName, new UpdateStoreQueryParams().setHybridRewindSeconds(10).setHybridOffsetLagThreshold(10));
    }

    cluster.createVersion(storeName, keySchema, valueSchema, Stream.of());

    // Create an AVRO record
    Schema valueRecordSchema = Schema.parse(valueSchema);
    Schema intArraySchema = valueRecordSchema.getField("intArray").schema();

    ClientConfig config = ClientConfig
        .defaultGenericClientConfig(storeName)
        .setVeniceURL(cluster.getRandomRouterURL());

    SystemProducer producer = TestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.STREAM, optionalConfigs);

    try (AvroGenericStoreClient<String, GenericRecord> client = ClientFactory.getAndStartGenericAvroClient(config)) {
      // Send the record to Venice using the SystemProducer
      GenericRecord record = new GenericData.Record(Schema.parse(valueSchema));
      record.put("string", "somestring");
      record.put("number", 3.14);
      record.put("intArray", new GenericData.Array<>(intArraySchema, Collections.singletonList(1)));

      TestPushUtils.sendStreamingRecord(producer, storeName, "keystring", record);

      // Verify we got the right record
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        GenericRecord recordFromVenice;
        try {
          recordFromVenice = client.get("keystring").get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        assertNotNull(recordFromVenice, "Value for key: 'keystring' should not be null");

        Object stringField = recordFromVenice.get("string");
        assertNotNull(stringField, "'string' field should not be null");
        assertEquals(stringField.toString(), "somestring");

        Object numberField = recordFromVenice.get("number");
        assertNotNull(numberField, "'number' field should not be null");
        assertEquals(numberField, 3.14);

        Object intArrayField = recordFromVenice.get("intArray");
        assertEquals(intArrayField, new GenericData.Array<>(intArraySchema, Collections.singletonList(1)));
      });

      // Update the record
      Schema noOpSchema = writeComputeSchema.getField("number").schema().getTypes().get(0);
      GenericData.Record noOpRecord = new GenericData.Record(noOpSchema);

      GenericData.Record collectionUpdateRecord =
          new GenericData.Record(writeComputeSchema.getField("intArray").schema().getTypes().get(1));
      collectionUpdateRecord.put(SET_UNION, Collections.singletonList(2));
      collectionUpdateRecord.put(SET_DIFF, Collections.singletonList(1));

      GenericRecord partialUpdateRecord = new GenericData.Record(writeComputeSchema);
      partialUpdateRecord.put("number", noOpRecord);
      partialUpdateRecord.put("string", "updatedString");
      partialUpdateRecord.put("intArray", collectionUpdateRecord);

      TestPushUtils.sendStreamingRecord(producer, storeName, "keystring", partialUpdateRecord);

      // Verify the update
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        GenericRecord updatedRecord;
        try {
          updatedRecord = client.get("keystring").get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        assertEquals(updatedRecord.get("number"), 3.14);
        assertEquals(updatedRecord.get("string").toString(), "updatedString");
        assertEquals(updatedRecord.get("intArray"), new GenericData.Array<>(intArraySchema, Collections.singletonList(2)));
      });

      // Delete the record
      TestPushUtils.sendStreamingRecord(producer, storeName, "keystring", null);

      // Verify the delete
      TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
        GenericRecord deletedRecord = null;
        try {
          deletedRecord = client.get("keystring").get();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        assertNull(deletedRecord);
      });

    } finally {
      producer.stop();
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testSchemaMismatchError() throws Exception {
    String storeName = cluster.createStore(0);
    SystemProducer producer = TestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.BATCH);
    try {
      // Send a record with a wrong schema, this is byte[] of chars "1", "2", "3", expects int
      assertThrows(SamzaException.class, () -> TestPushUtils.sendStreamingRecord(producer, storeName, new byte[] {49, 50, 51}, 0));
    } finally {
      producer.stop();
    }
  }

  @DataProvider(name = "testSerializationParams")
  public static Object[][] testSerializationParams() {
    return new Object[][] {
        {5, 10, "\"int\""},
        {new Utf8("one"), new Utf8("two"), "\"string\""},
        {6L, 8L,"\"long\""},
        {9.12D, 12.45D, "\"double\""},
        {1.6F, 7.4F, "\"float\""},
        {ByteBuffer.wrap(new byte[] {0x1, 0x2, 0x3}), ByteBuffer.wrap(new byte[] {0xb, 0xc, 0xd}), "\"bytes\""},
        {true, false, "\"boolean\""},
    };
  }

  @Test(timeOut = TEST_TIMEOUT, dataProvider = "testSerializationParams")
  public void testSerialization(Object key, Object value, String schema) throws Exception {
    testSerializationCast(key, key, value, value, schema);
  }

  @DataProvider(name = "testSerializationCastParams")
  public static Object[][] testSerializationCastParams() {
    return new Object[][]{
        {
          new byte[]{0x3, 0x4, 0x5}, ByteBuffer.wrap(new byte[]{0x3, 0x4, 0x5}),
          new byte[]{0xd, 0xe, 0xf}, ByteBuffer.wrap(new byte[]{0xd, 0xe, 0xf}),
          "\"bytes\""
        },
        {
          "three", "three",
          "four", new Utf8("four"),
          "\"string\""
        },
    };
  }

  /**
   * Avro sometimes returns a different type when deserializing.  For example, a serialized String is deserialized
   * into a Utf8 object.  This method can be used to get around the resulting assertion error when the returned object
   * is equivalent but of a different class.
   */
  @Test(timeOut = TEST_TIMEOUT, dataProvider = "testSerializationCastParams")
  public void testSerializationCast(Object writeKey, Object readKey, Object value, Object expectedValue, String schema) throws Exception {
    String storeName = TestUtils.getUniqueString("schema-test-store");

    try (ControllerClient client = cluster.getControllerClient()) {
      client.createNewStore(storeName, "owner", schema, schema);

      SystemProducer producer = TestPushUtils.getSamzaProducer(cluster, storeName, Version.PushType.BATCH);
      try {
        TestPushUtils.sendStreamingRecord(producer, storeName, writeKey, value);
      } finally {
        producer.stop();
      }

      client.writeEndOfPush(storeName, 1);
      cluster.waitVersion(storeName, 1);
    }

    ClientConfig config = ClientConfig
        .defaultGenericClientConfig(storeName)
        .setVeniceURL(cluster.getRandomRouterURL());

    try (AvroGenericStoreClient<Object, Object> client = ClientFactory.getAndStartGenericAvroClient(config)) {
      Object actualValue = client.get(readKey).get();
      assertEquals(actualValue, expectedValue,
          "Expected [" + expectedValue + "] of type " + expectedValue.getClass() + " but found [" + actualValue + "] of type " + actualValue.getClass());
    }
  }
}
