package com.linkedin.venice.endToEnd;

import static com.linkedin.venice.ConfigKeys.DEFAULT_MAX_NUMBER_OF_PARTITIONS;
import static com.linkedin.venice.ConfigKeys.NATIVE_REPLICATION_SOURCE_FABRIC;
import static com.linkedin.venice.ConfigKeys.PARENT_KAFKA_CLUSTER_FABRIC_LIST;
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
import com.linkedin.venice.storage.protocol.ChunkedValueManifest;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.writer.VeniceWriter;
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
 * End-to-end coverage for nearline large-record write skipping on an Active/Active + write-compute store.
 *
 * <p>Partial updates from the realtime topic are the only way a record can grow past the record size limit, because
 * the realtime path has no chunking. The limit is enforced <em>after the fact</em>: the write that first takes a
 * record over {@code maxNearlineRecordSizeBytes} is allowed through, and every partial update after it is skipped
 * until a full put or a delete resets the key. The skip is read straight off the stored record's chunk manifest, so
 * an oversized record costs nothing to skip — no chunk is fetched, assembled, merged or re-chunked. Ingestion is
 * never paused, so unrelated keys are unaffected — that property is asserted here too, since a previous attempt at
 * this feature paused whole partitions and turned one pathological key into a store-wide outage.
 *
 * <p>The skip decision is made independently by the leader in each region, so every assertion is made against both
 * regions.
 *
 * <p>Determinism note: a skipped write is a non-event, and "the value did not change" is indistinguishable from "the
 * value has not changed *yet*". Every test therefore sends a sentinel write to a second key from the same producer
 * after the oversized write. The store has a single partition, so the two keys share a partition and are consumed in
 * order; once the sentinel is visible in a region, the oversized write ahead of it has necessarily been processed
 * there.
 *
 * <p>All tests share one store, because the empty push that brings it online dominates the runtime. They are isolated
 * from each other by writing to a distinct key, which is sufficient because each write is judged on its own key.
 */
public class TestNearlineLargeRecordSkipping extends AbstractMultiRegionTest {
  private static final Logger LOGGER = LogManager.getLogger(TestNearlineLargeRecordSkipping.class);
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
  /**
   * Small enough that an oversized test record is split into several chunks and stored behind a manifest, but large
   * enough that the servers' routine internal writes (system store pushes, heartbeats) still fit in a single message.
   * Dropping much below this starves system store pushes and makes cluster setup time out.
   */
  private static final int CHUNK_SIZE = 16 * 1024;
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
   * the stream — which would make the "this write was dropped" assertions below nondeterministic. Skipping is decided
   * by the leader, and the leader is still exercised in both regions, so replication factor is orthogonal here.
   */
  @Override
  protected int getReplicationFactor() {
    return 1;
  }

  /**
   * Force chunking at a small size so that a stored value can be made to exceed the nearline limit without building
   * megabyte-sized records. Chunking is what causes a value to be stored as a {@link ChunkedValueManifest}, and the
   * manifest's {@code size} field is what lets an already-oversized record be rejected without being assembled.
   */
  @Override
  protected Properties getExtraServerProperties() {
    Properties serverProps = new Properties();
    serverProps.setProperty(VeniceWriter.MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES, String.valueOf(CHUNK_SIZE));
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
   * isolated by using a distinct key each, which is sufficient because each write is judged on its own key.
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
   * The core behavior. The write that takes the record over the limit is deliberately allowed through; it is every
   * partial update after it that is skipped, in every region, from the stored chunk manifest alone.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testUpdatesAfterARecordCrossesTheLimitAreSkippedInAllRegions() {
    String key = "key-skipped-in-all-regions";
    sendPartialUpdate(0, key, "compliant", generatePayload(SMALL_PAYLOAD_LENGTH));
    waitForRecordInAllRegions(key, "compliant", SMALL_PAYLOAD_LENGTH);

    // Allowed through: rejecting it would leave a compliant value in storage, so every later update would keep
    // paying the full read-assemble-merge cost instead of being short-circuited by the manifest.
    sendPartialUpdate(0, key, "oversized", generatePayload(OVERSIZED_PAYLOAD_LENGTH));
    waitForRecordInAllRegions(key, "oversized", OVERSIZED_PAYLOAD_LENGTH);

    sendPartialUpdate(0, key, "must-be-skipped", generatePayload(SMALL_PAYLOAD_LENGTH));
    awaitSentinel("after-skipped-write");
    assertRecordInAllRegions(
        key,
        "oversized",
        OVERSIZED_PAYLOAD_LENGTH,
        "Once the stored record is oversized, later partial updates must be skipped and leave it untouched");
  }

  /**
   * Skipping must not stop the world. A previous iteration of this feature paused consumption on every partition when
   * a single key was too large, which is precisely the multitenant failure mode this design avoids.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testUnrelatedKeysKeepIngestingWhileAKeyIsSkipped() {
    String skippedKey = "key-skipped-while-others-ingest";
    makeStoredRecordExceedTheLimit(skippedKey);
    sendPartialUpdate(0, skippedKey, "must-be-skipped", generatePayload(SMALL_PAYLOAD_LENGTH));
    awaitSentinel("first-write-after-skip");

    String unrelatedKey = "unrelated-key";
    for (int i = 0; i < 3; i++) {
      String name = "healthy-" + i;
      sendPartialUpdate(0, unrelatedKey, name, generatePayload(SMALL_PAYLOAD_LENGTH));
      waitForRecordInAllRegions(unrelatedKey, name, SMALL_PAYLOAD_LENGTH);
    }

    assertRecordInAllRegions(
        skippedKey,
        "grown",
        OVERSIZED_PAYLOAD_LENGTH,
        "The oversized key must still be skipped after unrelated keys kept ingesting");
  }

  /**
   * The scenario the size limit is really rolled out for: a record that is already over the limit, because it was
   * grown before the limit applied to it. It is stored chunked, so its size is read straight off the
   * {@link ChunkedValueManifest} and partial updates against it are rejected without ever assembling the chunks. A
   * full put is one of the two documented ways to reset such a record, so it must be applied and must make the key
   * writable again.
   */
  @Test(timeOut = TEST_TIMEOUT)
  public void testFullPutResetsAnAlreadyOversizedRecordAndRestoresPartialUpdates() {
    String key = "key-reset-by-full-put";
    makeStoredRecordExceedTheLimit(key);

    GenericRecord resetValue = new GenericData.Record(valueSchema);
    resetValue.put(NAME_FIELD, "reset-by-put");
    resetValue.put(PAYLOAD_FIELD, generatePayload(SMALL_PAYLOAD_LENGTH));
    sendStreamingRecord(systemProducerMap.get(0), storeName, key, resetValue);
    waitForRecordInAllRegions(key, "reset-by-put", SMALL_PAYLOAD_LENGTH);

    sendPartialUpdate(0, key, "writable-again", generatePayload(SMALL_PAYLOAD_LENGTH));
    waitForRecordInAllRegions(key, "writable-again", SMALL_PAYLOAD_LENGTH);
  }

  /** The other documented reset path: a delete must make the key writable again so it can be rebuilt from scratch. */
  @Test(timeOut = TEST_TIMEOUT)
  public void testDeleteResetsAnAlreadyOversizedRecordAndRestoresPartialUpdates() {
    String key = "key-reset-by-delete";
    makeStoredRecordExceedTheLimit(key);

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
   * Leaves the key holding a stored record that is over the nearline limit and large enough to be chunked, so it is
   * stored behind a {@link ChunkedValueManifest} and subsequent partial updates against it are skipped from that
   * manifest alone. The growing write is simply allowed through, which is the designed behavior.
   */
  private void makeStoredRecordExceedTheLimit(String key) {
    sendPartialUpdate(0, key, "grown", generatePayload(OVERSIZED_PAYLOAD_LENGTH));
    waitForRecordInAllRegions(key, "grown", OVERSIZED_PAYLOAD_LENGTH);
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
