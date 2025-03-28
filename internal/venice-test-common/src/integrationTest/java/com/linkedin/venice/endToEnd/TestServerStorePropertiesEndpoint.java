package com.linkedin.venice.endToEnd;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientResponse;
import com.linkedin.venice.controller.Admin;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.metadata.payload.StorePropertiesPayloadRecord;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestServerStorePropertiesEndpoint extends AbstractClientEndToEndSetup {
  private static final Logger LOGGER = LogManager.getLogger(TestServerStorePropertiesEndpoint.class);
  private static final int TIME_OUT = 120 * Time.MS_PER_SECOND;

  private Random RANDOM;

  private Optional<SSLFactory> sslFactory;
  private VeniceServerWrapper veniceServerWrapper;
  private String serverUrl;

  @BeforeClass(alwaysRun = true)
  public void beforeClassServerStoreProperties() {
    long seed = System.nanoTime();
    RANDOM = new Random(seed);
    LOGGER.info("Random seed set: {}", seed);
  }

  @BeforeMethod(alwaysRun = true)
  public void beforeMethodServerStoreProperties() {
    sslFactory = Optional.of(SslUtils.getVeniceLocalSslFactory());
    veniceServerWrapper = veniceCluster.getVeniceServers().stream().findAny().get();
    serverUrl = "https://" + veniceServerWrapper.getHost() + ":" + veniceServerWrapper.getPort();
  }

  @Test(timeOut = TIME_OUT)
  public void testRequestBasedStoreProperties() throws Exception {
    Admin admin = veniceCluster.getLeaderVeniceController().getVeniceAdmin();
    Optional<Integer> largestKnownSchemaId = Optional.empty();

    StorePropertiesPayloadRecord record = getStorePropertiesPayloadRecord(storeName, largestKnownSchemaId);

    assertNotNull(record);
    assertNotNull(record.helixGroupInfo);
    assertNotNull(record.routingInfo);

    StoreMetaValue storeMetaValue = deserializeStoreMetaValue(record);
    assertStorePropertiesPayloadRecord(storeMetaValue, admin, storeName, largestKnownSchemaId);
  }

  @Test(timeOut = TIME_OUT)
  public void testRequestBasedStorePropertiesWithStoreChanges() throws Exception {

    String clusterName = veniceCluster.getClusterName();
    String owner = Long.toString(RANDOM.nextLong());
    int largestUsedVersion = RANDOM.nextInt();

    veniceCluster.getLeaderVeniceController().getVeniceAdmin().setStoreOwner(clusterName, storeName, owner);
    veniceCluster.getLeaderVeniceController()
        .getVeniceAdmin()
        .setStoreLargestUsedVersion(clusterName, storeName, largestUsedVersion);

    Optional<Integer> largestKnownSchemaId = Optional.empty();

    TestUtils.waitForNonDeterministicAssertion(TIME_OUT, TimeUnit.SECONDS, () -> {
      StorePropertiesPayloadRecord record = getStorePropertiesPayloadRecord(storeName, largestKnownSchemaId);
      assertNotNull(record);
      assertNotNull(record.helixGroupInfo);
      assertNotNull(record.routingInfo);

      StoreMetaValue storeMetaValue = deserializeStoreMetaValue(record);

      assertEquals(storeMetaValue.storeProperties.owner.toString(), owner);
      assertEquals(storeMetaValue.storeProperties.largestUsedVersionNumber, largestUsedVersion);

      Admin admin = veniceCluster.getLeaderVeniceController().getVeniceAdmin();
      assertStorePropertiesPayloadRecord(storeMetaValue, admin, storeName, largestKnownSchemaId);
    });

  }

  @Test(timeOut = TIME_OUT)
  public void testRequestBasedStorePropertiesWithLargestKnownSchemaId() throws Exception {

    String clusterName = veniceCluster.getClusterName();
    String owner = Long.toString(RANDOM.nextLong());
    int largestUsedVersion = RANDOM.nextInt();

    veniceCluster.getLeaderVeniceController().getVeniceAdmin().setStoreOwner(clusterName, storeName, owner);
    veniceCluster.getLeaderVeniceController()
        .getVeniceAdmin()
        .setStoreLargestUsedVersion(clusterName, storeName, largestUsedVersion);

    Optional<Integer> largestKnownSchemaId = Optional.of(1);

    TestUtils.waitForNonDeterministicAssertion(TIME_OUT, TimeUnit.SECONDS, () -> {
      StorePropertiesPayloadRecord record = getStorePropertiesPayloadRecord(storeName, largestKnownSchemaId);
      assertNotNull(record);
      assertNotNull(record.helixGroupInfo);
      assertNotNull(record.routingInfo);

      StoreMetaValue storeMetaValue = deserializeStoreMetaValue(record);

      assertEquals(storeMetaValue.storeProperties.owner.toString(), owner);
      assertEquals(storeMetaValue.storeProperties.largestUsedVersionNumber, largestUsedVersion);

      Admin admin = veniceCluster.getLeaderVeniceController().getVeniceAdmin();
      assertStorePropertiesPayloadRecord(storeMetaValue, admin, storeName, largestKnownSchemaId);
    });
  }

  private StorePropertiesPayloadRecord getStorePropertiesPayloadRecord(
      String _storeName,
      Optional<Integer> largestKnownSchemaId) throws Exception {
    StorePropertiesPayloadRecord record;

    // Request
    ClientConfig clientConfig = ClientConfig.defaultGenericClientConfig(_storeName).setVeniceURL(serverUrl);
    clientConfig.setSslFactory(sslFactory.get());
    try (TransportClient transportClient = ClientFactory.getTransportClient(clientConfig)) {
      String requestUrl = QueryAction.STORE_PROPERTIES.toString().toLowerCase() + "/" + _storeName;
      if (largestKnownSchemaId.isPresent()) {
        requestUrl += "/" + largestKnownSchemaId.get();
      }
      TransportClientResponse response = transportClient.get(requestUrl).get();

      // Deserialize
      Schema writerSchema = StorePropertiesPayloadRecord.SCHEMA$;
      RecordDeserializer<StorePropertiesPayloadRecord> recordDeserializer = FastSerializerDeserializerFactory
          .getFastAvroSpecificDeserializer(writerSchema, StorePropertiesPayloadRecord.class);
      record = recordDeserializer.deserialize(response.getBody());

      return record;
    }
  }

  private void assertStorePropertiesPayloadRecord(
      StoreMetaValue storeMetaValue,
      Admin admin,
      String _storeName,
      Optional<Integer> largestKnownSchemaId) {

    String clusterName = veniceCluster.getClusterName();
    Store store = admin.getStore(clusterName, _storeName);

    // Assert
    assertNotNull(storeMetaValue);
    assertNotNull(storeMetaValue.storeProperties);

    // Store Properties
    assertEquals(storeMetaValue.storeProperties.name.toString(), store.getName());
    assertEquals(storeMetaValue.storeProperties.owner.toString(), store.getOwner());
    assertEquals(storeMetaValue.storeProperties.createdTime, store.getCreatedTime());
    assertEquals(storeMetaValue.storeProperties.currentVersion, store.getCurrentVersion());
    assertEquals(storeMetaValue.storeProperties.partitionCount, store.getPartitionCount());
    assertEquals(storeMetaValue.storeProperties.lowWatermark, store.getLowWatermark());
    assertEquals(storeMetaValue.storeProperties.enableWrites, store.isEnableWrites());
    assertEquals(storeMetaValue.storeProperties.enableReads, store.isEnableReads());
    assertEquals(storeMetaValue.storeProperties.storageQuotaInByte, store.getStorageQuotaInByte());
    assertEquals(storeMetaValue.storeProperties.readQuotaInCU, store.getReadQuotaInCU());
    assertEquals(storeMetaValue.storeProperties.batchGetLimit, store.getBatchGetLimit());
    assertEquals(storeMetaValue.storeProperties.largestUsedVersionNumber, store.getLargestUsedVersionNumber());
    assertEquals(
        storeMetaValue.storeProperties.latestVersionPromoteToCurrentTimestamp,
        store.getLatestVersionPromoteToCurrentTimestamp());
    assertEquals(storeMetaValue.storeProperties.versions.size(), store.getVersions().size());
    assertEquals(storeMetaValue.storeProperties.systemStores.size(), store.getSystemStores().size());

    // Store Key Schemas
    assertNotNull(storeMetaValue.storeKeySchemas);
    assertNotNull(storeMetaValue.storeKeySchemas.keySchemaMap);
    for (Map.Entry<CharSequence, CharSequence> entry: storeMetaValue.storeKeySchemas.keySchemaMap.entrySet()) {
      SchemaEntry expectedKeySchemaEntry = admin.getKeySchema(veniceCluster.getClusterName(), storeName);

      String actual = entry.getValue().toString();
      String expected = expectedKeySchemaEntry.getSchema().toString();
      assertEquals(actual, expected);
    }

    // Store Value Schemas
    for (Map.Entry<CharSequence, CharSequence> entry: storeMetaValue.storeValueSchemas.valueSchemaMap.entrySet()) {
      int valueSchemaId = Integer.parseInt(entry.getKey().toString());
      SchemaEntry expectedValueSchemaEntry =
          admin.getValueSchema(veniceCluster.getClusterName(), storeName, valueSchemaId);

      String actual = entry.getValue().toString();
      String expected = expectedValueSchemaEntry.getSchema().toString();
      assertEquals(actual, expected);
      if (largestKnownSchemaId.isPresent()) {
        assertTrue(largestKnownSchemaId.get() < valueSchemaId);
      }
    }

  }

  private StoreMetaValue deserializeStoreMetaValue(StorePropertiesPayloadRecord record) {
    Schema writerSchema = StoreMetaValue.SCHEMA$;
    RecordDeserializer<StoreMetaValue> recordDeserializer =
        FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(writerSchema, StoreMetaValue.class);
    return recordDeserializer.deserialize(record.storeMetaValueAvro);
  }
}
