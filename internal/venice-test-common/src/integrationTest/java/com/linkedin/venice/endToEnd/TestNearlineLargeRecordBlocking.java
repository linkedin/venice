package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
import static com.linkedin.venice.ConfigKeys.SERVER_NEARLINE_LARGE_RECORD_BLOCKING_ENABLED;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapperConstants.DEFAULT_PARENT_DATA_CENTER_REGION_NAME;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.sendStreamingRecord;
import static com.linkedin.venice.utils.TestUtils.assertCommand;
import static com.linkedin.venice.utils.TestUtils.waitForNonDeterministicAssertion;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.VeniceMultiClusterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.update.UpdateBuilder;
import com.linkedin.venice.writer.update.UpdateBuilderImpl;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * End-to-end coverage for nearline large-record write blocking on an Active/Active + write-compute store.
 *
 * <p>Partial updates from the realtime topic are the only way a record can grow past the record size limit, because
 * the realtime path has no chunking. Once a partial update would push the assembled value over
 * {@code maxNearlineRecordSizeBytes}, the server drops that write instead of producing it, and keeps dropping partial
 * updates to that key until a full put or a delete resets it. Ingestion is never paused, so unrelated keys are
 * unaffected — that property is asserted here too, since a previous attempt at this feature paused whole partitions
 * and turned one pathological key into a store-wide outage.
 *
 * <p>The blocking decision is made independently by the leader in each region, so every assertion is made against both
 * regions.
 *
 * <p>Determinism note: a blocked write is a non-event, and "the value did not change" is indistinguishable from "the
 * value has not changed *yet*". Every test therefore sends a sentinel write to a second key from the same producer
 * after the oversized write. The store has a single partition, so the two keys share a partition and are consumed in
 * order; once the sentinel is visible in a region, the oversized write ahead of it has necessarily been processed
 * there.
 *
 * <p>All tests share one store, because the empty push that brings it online dominates the runtime. They are isolated
 * from each other by writing to a distinct key, which is sufficient because blocking is tracked per key.
 */
public class TestNearlineLargeRecordBlocking extends AbstractMultiRegionTest {
  private static final Logger LOGGER = LogManager.getLogger(TestNearlineLargeRecordBlocking.class);
  private static final int TEST_TIMEOUT = 3 * Time.MS_PER_MINUTE;
  /**
   * An empty push on a hybrid store is only reported {@code COMPLETED} once buffer replay has caught up, which is
   * markedly slower than the push itself. This runs once per class rather than inside a test method, so it is not
   * bounded by {@link #TEST_TIMEOUT} and can afford a generous allowance.
   */
  private static final int PUSH_TIMEOUT = 5 * Time.MS_PER_MINUTE;

  private static final String KEY_SCHEMA_STR = "{\"type\":\"string\"}";
  private static final String VALUE_SCHEMA_STR = "{\"type\":\"record\",\"name\":\"TestValue\",\"fields\":["
      + "{\"name\":\"name\",\"type\":\"string\",\"default\":\"\"},"
      + "{\"name\":\"payload\",\"type\":\"string\",\"default\":\"\"}]}";
  private static final String NAME_FIELD = "name";
  private static final String PAYLOAD_FIELD = "payload";

  /**
   * Deliberately far below the 1MB chunking threshold. The store-level override has no lower bound (only the
   * fleet-wide {@code default.max.record.size.bytes} is validated at >= 1MB), so a small limit exercises exactly the
   * same code path without making the test build multi-megabyte records.
   */
  private static final int MAX_NEARLINE_RECORD_SIZE_BYTES = 10 * 1024;
  private static final int SMALL_PAYLOAD_LENGTH = 512;
  private static final int OVERSIZED_PAYLOAD_LENGTH = 50 * 1024;

  private static final String SENTINEL_KEY = "sentinel-key";

  private Schema valueSchema;
  private Schema writeComputeSchema;

  private ControllerClient parentControllerClient;
  private List<ControllerClient> dcControllerClientList;
  private List<String> routerUrls;

  private String storeName;
  private Map<Integer, VeniceSystemProducer> systemProducerMap;
  private Map<String, AvroGenericStoreClient<String, GenericRecord>> storeClients;

  @Override
  protected Properties getExtraControllerProperties() {
    Properties controllerProps = new Properties();
    controllerProps.put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 1);
    controllerProps.put(NATIVE_REPLICATION_SOURCE_FABRIC, "dc-0");
    controllerProps.put(PARENT_KAFKA_CLUSTER_FABRIC_LIST, DEFAULT_PARENT_DATA_CENTER_REGION_NAME);
    return controllerProps;
  }

  /**
   * One replica per region, so that a read is served by exactly one server. A follower lags its leader by a version
   * topic round trip, so with more than one replica two consecutive reads can land on replicas at different points in
   * the stream — which would make the "this write was dropped" assertions below nondeterministic. Blocking is decided
   * by the leader, and the leader is still exercised in both regions, so replication factor is orthogonal here.
   */
  @Override
  protected int getReplicationFactor() {
    return 1;
  }

  @Override
  protected Properties getExtraServerProperties() {
    Properties serverProps = new Properties();
    serverProps.setProperty(SERVER_NEARLINE_LARGE_RECORD_BLOCKING_ENABLED, "true");
    return serverProps;
  }

  @Override
  @BeforeClass(alwaysRun = true)
  public void setUp() {
    super.setUp();
    valueSchema = AvroCompatibilityHelper.parse(VALUE_SCHEMA_STR);
    writeComputeSchema = WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(valueSchema);
    parentControllerClient = new ControllerClient(CLUSTER_NAME, getParentControllerUrl());
    dcControllerClientList = new ArrayList<>(childDatacenters.size());
    routerUrls = new ArrayList<>(childDatacenters.size());
    for (VeniceMultiClusterWrapper childDatacenter: childDatacenters) {
      dcControllerClientList.add(new ControllerClient(CLUSTER_NAME, childDatacenter.getControllerConnectString()));
      routerUrls.add(childDatacenter.getClusters().get(CLUSTER_NAME).getRandomRouterURL());
    }
    storeClients = new HashMap<>(childDatacenters.size());
    systemProducerMap = new HashMap<>(childDatacenters.size());
    createStoreAndPushInitialVersion();
  }

  /**
   * One store and one empty push for the whole class. An empty push on a hybrid store only reaches {@code COMPLETED}
   * after buffer replay catches up, so it is both slow and the most timing-sensitive part of the setup; doing it once
   * per test method made the class spend most of its time pushing and occasionally time out mid-replay. Tests are
   * isolated by using a distinct key each, which is sufficient because blocking state is tracked per key.
   */
  private void createStoreAndPushInitialVersion() {
    storeName = Utils.getUniqueString("test-store-nearline-large-record");
    assertCommand(parentControllerClient.createNewStore(storeName, "owner", KEY_SCHEMA_STR, VALUE_SCHEMA_STR));
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setNativeReplicationEnabled(true)
        .setActiveActiveReplicationEnabled(true)
        .setWriteComputationEnabled(true)
        .setChunkingEnabled(true)
        .setRmdChunkingEnabled(true)
        .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
        .setPartitionCount(1)
        .setHybridRewindSeconds(25L)
        .setHybridOffsetLagThreshold(1L)
        .setMaxNearlineRecordSizeBytes(MAX_NEARLINE_RECORD_SIZE_BYTES);
    assertCommand(parentControllerClient.updateStore(storeName, params));

    assertCommand(
        parentControllerClient.sendEmptyPushAndWait(storeName, Utils.getUniqueString("empty-push"), 1L, PUSH_TIMEOUT),
        "Empty push did not complete in " + PUSH_TIMEOUT + " ms");
    for (ControllerClient dcClient: dcControllerClientList) {
      waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
        StoreResponse storeResponse = assertCommand(dcClient.getStore(storeName));
        assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
        assertEquals(
            storeResponse.getStore().getMaxNearlineRecordSizeBytes(),
            MAX_NEARLINE_RECORD_SIZE_BYTES,
            "The nearline size limit must have replicated before the test writes anything");
      });
    }

    for (int dcId = 0; dcId < childDatacenters.size(); dcId++) {
      systemProducerMap.put(
          dcId,
          IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionMultiClusterWrapper, dcId, storeName));
    }
  }

  @Override
  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    if (systemProducerMap != null) {
      systemProducerMap.values().forEach(Utils::closeQuietlyWithErrorLogged);
    }
    if (storeClients != null) {
      storeClients.values().forEach(Utils::closeQuietlyWithErrorLogged);
    }
    try {
      if (storeName != null) {
        parentControllerClient.disableAndDeleteStore(storeName);
      }
    } catch (Exception e) {
      LOGGER.info("Best-effort store cleanup failed: {}", e.getLocalizedMessage());
    }
    if (dcControllerClientList != null) {
      dcControllerClientList.forEach(Utils::closeQuietlyWithErrorLogged);
    }
    Utils.closeQuietlyWithErrorLogged(parentControllerClient);
    super.cleanUp();
  }

  /**
   * The core behavior: a partial update that would push the record over the limit is dropped in every region, the
   * record stays at its last compliant value, and subsequent partial updates to the same key stay blocked even when
   * they are individually small — the record is already over the limit, so only a reset can bring it back.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testOversizedPartialUpdateIsBlockedInAllRegions() {
    String key = "key-blocked-in-all-regions";
    sendPartialUpdate(0, key, "compliant", generatePayload(SMALL_PAYLOAD_LENGTH));
    waitForRecordInAllRegions(key, "compliant", SMALL_PAYLOAD_LENGTH);

    sendPartialUpdate(0, key, "oversized", generatePayload(OVERSIZED_PAYLOAD_LENGTH));
    awaitSentinel("after-oversized-write");

    assertRecordInAllRegions(
        key,
        "compliant",
        SMALL_PAYLOAD_LENGTH,
        "The oversized partial update must have been dropped, leaving the last compliant value in place");

    // A small follow-up update must also be rejected: the key is blocked, not merely this one write.
    sendPartialUpdate(0, key, "small-follow-up", generatePayload(SMALL_PAYLOAD_LENGTH));
    awaitSentinel("after-follow-up-write");
    assertRecordInAllRegions(
        key,
        "compliant",
        SMALL_PAYLOAD_LENGTH,
        "Partial updates must stay blocked until the record is reset by a full put or a delete");
  }

  /**
   * Blocking must not stop the world. A previous iteration of this feature paused consumption on every partition when
   * a single key was too large, which is precisely the multitenant failure mode this design avoids.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testUnrelatedKeysKeepIngestingWhileAKeyIsBlocked() {
    String blockedKey = "key-blocked-while-others-ingest";
    sendPartialUpdate(0, blockedKey, "oversized", generatePayload(OVERSIZED_PAYLOAD_LENGTH));
    awaitSentinel("first-write-after-block");

    String unrelatedKey = "unrelated-key";
    for (int i = 0; i < 3; i++) {
      String name = "healthy-" + i;
      sendPartialUpdate(0, unrelatedKey, name, generatePayload(SMALL_PAYLOAD_LENGTH));
      waitForRecordInAllRegions(unrelatedKey, name, SMALL_PAYLOAD_LENGTH);
    }

    assertRecordInAllRegions(blockedKey, null, -1, "The oversized key must never have been written at all");
  }

  /**
   * A full put is one of the two documented ways to reset an oversized record, so it must always be applied and must
   * restore partial updates for the key. Blocking it would make an oversized record permanently unwritable.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testFullPutResetsAnOversizedRecordAndRestoresPartialUpdates() {
    String key = "key-reset-by-full-put";
    establishBlockedKey(key);

    GenericRecord resetValue = new GenericData.Record(valueSchema);
    resetValue.put(NAME_FIELD, "reset-by-put");
    resetValue.put(PAYLOAD_FIELD, generatePayload(SMALL_PAYLOAD_LENGTH));
    sendStreamingRecord(systemProducerMap.get(0), storeName, key, resetValue);
    waitForRecordInAllRegions(key, "reset-by-put", SMALL_PAYLOAD_LENGTH);

    sendPartialUpdate(0, key, "writable-again", generatePayload(SMALL_PAYLOAD_LENGTH));
    waitForRecordInAllRegions(key, "writable-again", SMALL_PAYLOAD_LENGTH);
  }

  /** The other documented reset path: a delete must clear the block so the key can be rebuilt from scratch. */
  @Test(timeOut = TEST_TIMEOUT)
  public void testDeleteResetsAnOversizedRecordAndRestoresPartialUpdates() {
    String key = "key-reset-by-delete";
    establishBlockedKey(key);

    sendStreamingRecord(systemProducerMap.get(0), storeName, key, null);
    waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
      for (String routerUrl: routerUrls) {
        assertNull(readRecord(routerUrl, key), "The delete must have been applied in " + routerUrl);
      }
    });

    sendPartialUpdate(0, key, "rebuilt", generatePayload(SMALL_PAYLOAD_LENGTH));
    waitForRecordInAllRegions(key, "rebuilt", SMALL_PAYLOAD_LENGTH);
  }

  /**
   * Drives the key over the limit from a known-good starting value, and confirms the block took effect, so the reset
   * tests start from an unambiguous state.
   */
  private void establishBlockedKey(String key) {
    sendPartialUpdate(0, key, "compliant", generatePayload(SMALL_PAYLOAD_LENGTH));
    waitForRecordInAllRegions(key, "compliant", SMALL_PAYLOAD_LENGTH);

    sendPartialUpdate(0, key, "oversized", generatePayload(OVERSIZED_PAYLOAD_LENGTH));
    awaitSentinel("establish-block");
    assertRecordInAllRegions(
        key,
        "compliant",
        SMALL_PAYLOAD_LENGTH,
        "The key must be blocked before the reset is exercised");
  }

  /**
   * Writes a unique sentinel value to a second key from the same producer and waits for it in every region. Because
   * the store has a single partition, the sentinel is consumed after everything sent before it, so its arrival proves
   * the preceding write has been processed — including a write that was dropped and therefore left no trace.
   */
  private void awaitSentinel(String marker) {
    String uniqueName = marker + "-" + Utils.getUniqueString("marker");
    sendPartialUpdate(0, SENTINEL_KEY, uniqueName, generatePayload(SMALL_PAYLOAD_LENGTH));
    waitForRecordInAllRegions(SENTINEL_KEY, uniqueName, SMALL_PAYLOAD_LENGTH);
  }

  private void sendPartialUpdate(int dcId, String key, String name, String payload) {
    UpdateBuilder updateBuilder = new UpdateBuilderImpl(writeComputeSchema);
    updateBuilder.setNewFieldValue(NAME_FIELD, name);
    updateBuilder.setNewFieldValue(PAYLOAD_FIELD, payload);
    sendStreamingRecord(systemProducerMap.get(dcId), storeName, key, updateBuilder.build());
  }

  private void waitForRecordInAllRegions(String key, String expectedName, int expectedPayloadLength) {
    waitForNonDeterministicAssertion(
        60,
        TimeUnit.SECONDS,
        true,
        () -> assertRecordInAllRegions(key, expectedName, expectedPayloadLength, "Write did not replicate"));
  }

  /**
   * @param expectedName the expected {@code name} field, or {@code null} to assert the key is absent
   */
  private void assertRecordInAllRegions(String key, String expectedName, int expectedPayloadLength, String message) {
    for (String routerUrl: routerUrls) {
      GenericRecord record = readRecord(routerUrl, key);
      if (expectedName == null) {
        assertNull(record, message + " [key=" + key + ", region=" + routerUrl + "]");
        continue;
      }
      assertNotNull(record, message + " [key=" + key + ", region=" + routerUrl + "]");
      assertEquals(
          record.get(NAME_FIELD).toString(),
          expectedName,
          message + " [key=" + key + ", region=" + routerUrl + "]");
      // Compared by length rather than by content, so a failure message stays readable for a 50KB payload.
      assertEquals(
          record.get(PAYLOAD_FIELD).toString().length(),
          expectedPayloadLength,
          message + " [key=" + key + ", region=" + routerUrl + ", payload length mismatch]");
    }
  }

  private GenericRecord readRecord(String routerUrl, String key) {
    try {
      return getStoreClient(routerUrl).get(key).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private AvroGenericStoreClient<String, GenericRecord> getStoreClient(String routerUrl) {
    return storeClients.computeIfAbsent(
        routerUrl,
        k -> ClientFactory
            .getAndStartGenericAvroClient(ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerUrl)));
  }

  /** Non-repeating content, so the assertions hold regardless of whether compression is in play. */
  private static String generatePayload(int length) {
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append((char) ('a' + (i % 26)));
    }
    return sb.toString();
  }
}
