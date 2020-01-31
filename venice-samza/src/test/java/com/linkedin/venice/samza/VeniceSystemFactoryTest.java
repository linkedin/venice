package com.linkedin.venice.samza;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.schema.WriteComputeSchemaAdapter;
import com.linkedin.venice.utils.TestUtils;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.CommonConfigKeys.*;
import static com.linkedin.venice.samza.VeniceSystemFactory.*;
import static com.linkedin.venice.schema.WriteComputeSchemaAdapter.*;
import static org.testng.Assert.*;


public class VeniceSystemFactoryTest {

  private static final String VALUE_SCHEMA = "{\n" +
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

  private static final String KEY_SCHEMA = "\"string\"";
  private static final String VENICE_SYSTEM_NAME = "venice"; //This is the Samza system name for use by the Samza API.
  private VeniceClusterWrapper venice;
  private D2ControllerClient client;
  private String zkAddress;


  @BeforeClass
  private void setUp() {
    venice = ServiceFactory.getVeniceCluster();
    zkAddress = venice.getZk().getAddress();
    client = new D2ControllerClient(D2TestUtils.CONTROLLER_SERVICE_NAME, venice.getClusterName(), zkAddress, Optional.empty());
  }

  @AfterClass
  private void tearDown() {
    client.close();
    IOUtils.closeQuietly(venice);
  }

  /**
   * Write a record using the Samza SystemProducer for Venice, then verify we can read that record.
   */
  @Test
  public void testGetProducer() {
    String storeName = TestUtils.getUniqueString("store");

    client.createNewStore(storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);

    Schema writeComputeSchema = WriteComputeSchemaAdapter.parse(VALUE_SCHEMA);

    //generate write compute schema
    client.addDerivedSchema(storeName, 1, writeComputeSchema.toString());

    // Enable hybrid
    client.updateStore(storeName, new UpdateStoreQueryParams().setHybridRewindSeconds(10).setHybridOffsetLagThreshold(10));

    //Configure and create a SystemProducer for Venice
    client.emptyPush(storeName, TestUtils.getUniqueString(storeName), 10000);

    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> client.getStore(storeName).getStore().getCurrentVersion() == 1);

    SystemProducer veniceProducer = getVeniceProducer(Version.PushType.STREAM, storeName);

    //Create an AVRO record
    Schema valueRecordSchema = Schema.parse(VALUE_SCHEMA);
    Schema intArraySchema = valueRecordSchema.getField("intArray").schema();

    GenericRecord record = new GenericData.Record(Schema.parse(VALUE_SCHEMA));
    record.put("string", "somestring");
    record.put("number", 3.14);
    record.put("intArray", new GenericData.Array<>(intArraySchema, Collections.singletonList(1)));
    OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(
        new SystemStream(VENICE_SYSTEM_NAME, storeName),
        "keystring",
        record);

    //Send the record to Venice using the SystemProducer
    veniceProducer.send(storeName, envelope);

    //read the record out of Venice
    AvroGenericStoreClient<String, GenericRecord> storeClient =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()));

    //verify we got the right record
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      GenericRecord recordFromVenice;
      try {
        recordFromVenice = storeClient.get("keystring").get(1, TimeUnit.SECONDS);
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

    //update the record
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
    OutgoingMessageEnvelope partialUpdateEnvelope = new OutgoingMessageEnvelope(
        new SystemStream(VENICE_SYSTEM_NAME, storeName),
        "keystring",
        partialUpdateRecord);

    veniceProducer.send(storeName, partialUpdateEnvelope);

    //verify the update
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      GenericRecord updatedRecord;
      try {
        updatedRecord = storeClient.get("keystring").get(1, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      assertEquals(updatedRecord.get("number"), 3.14);
      assertEquals(updatedRecord.get("string").toString(), "updatedString");
      assertEquals(updatedRecord.get("intArray"), new GenericData.Array<>(intArraySchema, Collections.singletonList(2)));
    });

    //delete the record
    OutgoingMessageEnvelope deleteEnvelope = new OutgoingMessageEnvelope(
        new SystemStream(VENICE_SYSTEM_NAME, storeName),
        "keystring",
        null);
    veniceProducer.send(storeName, deleteEnvelope);

    //verify the delete
    TestUtils.waitForNonDeterministicAssertion(10, TimeUnit.SECONDS, () -> {
      GenericRecord deletedRecord = null;
      try {
        deletedRecord = storeClient.get("keystring").get(1, TimeUnit.SECONDS);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      Assert.assertNull(deletedRecord);
    });

    veniceProducer.stop();
    storeClient.close();
  }

  @Test
  public void testAllSerialization() throws InterruptedException, ExecutionException, TimeoutException {
    testSerialization(5, 10,"\"int\"");
    testSerialization(new Utf8("one"), new Utf8("two"), "\"string\"");
    testSerializationCast("three", "three", "four", new Utf8("four"), "\"string\"");
    testSerialization(6L, 8L,"\"long\"");
    testSerialization(9.12D, 12.45D,"\"double\"");
    testSerialization(1.6F, 7.4F,"\"float\"");
    testSerialization(ByteBuffer.wrap(new byte[] {0x1, 0x2, 0x3}), ByteBuffer.wrap(new byte[] {0xb, 0xc, 0xd}),"\"bytes\"");
    testSerializationCast(new byte[] {0x3, 0x4, 0x5}, ByteBuffer.wrap(new byte[] {0x3, 0x4, 0x5}), new byte[] {0xd, 0xe, 0xf}, ByteBuffer.wrap(new byte[] {0xd, 0xe, 0xf}),"\"bytes\"");
    testSerialization(true, false,"\"boolean\"");
  }

  @Test
  public void testSchemaMismatchError() {
    String storeName = TestUtils.getUniqueString("store");
    client.createNewStore(storeName, "owner", KEY_SCHEMA, VALUE_SCHEMA);
    SystemProducer veniceProducer = getVeniceProducer(Version.PushType.BATCH, storeName);
    //Create an AVRO record
    GenericRecord record = new GenericData.Record(Schema.parse(VALUE_SCHEMA));
    record.put("string", "somestring");
    record.put("number", 3.14);
    OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(
        new SystemStream(VENICE_SYSTEM_NAME, storeName),
        new byte[] {49, 50, 51}, //wrong schema, this is byte[] of chars "1", "2", "3", expects string
        record);
    try {
      veniceProducer.send(storeName, envelope);
      Assert.fail("Sending message with byte[] key when String expected must fail");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Key object: 123"));
    }
  }

  /**
   * Avro sometimes returns a different type when deserializing.  For example, a serialized String is deserialized
   * into a Utf8 object.  This method can be used to get around the resulting assertion error when the returned object
   * is equivalent but of a different class.
   */
  private <K1, K2, V1, V2> void testSerializationCast(K1 writeKey, K2 readKey, V1 value, V2 expectedValue, String schema)
      throws InterruptedException, ExecutionException, TimeoutException {
    String storeName = TestUtils.getUniqueString("schema-test-store");
    client.createNewStore(storeName, "owner", schema, schema);
    VeniceSystemProducer producer = (VeniceSystemProducer) getVeniceProducer(Version.PushType.BATCH, storeName);
    producer.send(storeName, new OutgoingMessageEnvelope(
        new SystemStream(VENICE_SYSTEM_NAME, storeName),
        writeKey,value));
    client.writeEndOfPush(storeName, 1);
    TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> client.getStore(storeName).getStore().getCurrentVersion() == 1);

    AvroGenericStoreClient<K2, V2> storeClient =
        ClientFactory.getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(venice.getRandomRouterURL()));
    V2 valueFromStore = storeClient.get(readKey).get(1, TimeUnit.SECONDS);
    Assert.assertEquals(valueFromStore, expectedValue,
        valueFromStore.toString() + " of type: " + valueFromStore.getClass()
            + " but expected " + value + " of type: " + value.getClass());
  }

  private <T> void testSerialization(T key, T value, String schema) throws InterruptedException, ExecutionException, TimeoutException {
    testSerializationCast(key, key, value, value, schema);
  }

  private SystemProducer getVeniceProducer(Version.PushType pushType, String storeName){
    Map<String, String> samzaConfig = new HashMap<>();
    String configPrefix = SYSTEMS_PREFIX + VENICE_SYSTEM_NAME + DOT;
    samzaConfig.put(configPrefix + VENICE_PUSH_TYPE, pushType.toString());
    samzaConfig.put(configPrefix + VENICE_STORE, storeName);
    samzaConfig.put(D2_ZK_HOSTS_PROPERTY, zkAddress);
    samzaConfig.put(VENICE_PARENT_D2_ZK_HOSTS, "invalid_parent_zk_address");
    samzaConfig.put(DEPLOYMENT_ID, TestUtils.getUniqueString("samza-push-id"));
    samzaConfig.put(SSL_ENABLED, "false");
    VeniceSystemFactory factory = new VeniceSystemFactory();
    SystemProducer veniceProducer = factory.getProducer(VENICE_SYSTEM_NAME, new MapConfig(samzaConfig), null);
    return veniceProducer;
  }
}