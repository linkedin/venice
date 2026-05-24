package com.linkedin.venice.controller;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.InvalidVeniceSchemaException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.status.protocol.PushJobDetails;
import com.linkedin.venice.status.protocol.PushJobStatusRecordKey;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.ConcurrencyUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Owns the state and Kafka/D2 plumbing used to read and write push job details and batch job
 * heartbeat values: lazily-created Venice writers against the push-job-details RT topic, lazy
 * router-backed Avro store clients for both system stores, and the schema-id resolution that
 * accompanies the writes.
 */
class PushJobDetailsManager implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(PushJobDetailsManager.class);
  private static final String PUSH_JOB_DETAILS_WRITER = "PUSH_JOB_DETAILS_WRITER";

  private final Admin admin;
  private final D2Client d2Client;
  private final boolean sslEnabled;
  private final String pushJobStatusStoreClusterName;
  private final boolean multiRegion;
  private final Optional<SSLFactory> sslFactory;

  private final InternalAvroSpecificSerializer<PushJobDetails> pushJobDetailsSerializer =
      AvroProtocolDefinition.PUSH_JOB_DETAILS.getSerializer();

  private final Map<String, VeniceWriter> jobTrackingVeniceWriterMap = new VeniceConcurrentHashMap<>();

  private final Object pushJobDetailsClientLock = new Object();
  private AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> pushJobDetailsStoreClient = null;

  private final Object livenessHeartbeatClientLock = new Object();
  private AvroSpecificStoreClient<BatchJobHeartbeatKey, BatchJobHeartbeatValue> livenessHeartbeatStoreClient = null;

  // Lazily initialized.
  private PubSubTopic pushJobDetailsRTTopic;
  private int pushJobDetailsSchemaId = -1;

  PushJobDetailsManager(
      Admin admin,
      D2Client d2Client,
      boolean sslEnabled,
      String pushJobStatusStoreClusterName,
      boolean multiRegion,
      Optional<SSLFactory> sslFactory) {
    this.admin = admin;
    this.d2Client = d2Client;
    this.sslEnabled = sslEnabled;
    this.pushJobStatusStoreClusterName = pushJobStatusStoreClusterName;
    this.multiRegion = multiRegion;
    this.sslFactory = sslFactory;
  }

  InternalAvroSpecificSerializer<PushJobDetails> getPushJobDetailsSerializer() {
    return pushJobDetailsSerializer;
  }

  /**
   * Write the push job details record onto the child controller's local push-job-details real-time
   * topic. Lazily resolves the topic and constructs the underlying Venice writer on first use.
   */
  void writeToLocalRTTopic(PushJobStatusRecordKey key, PushJobDetails value) {
    if (StringUtils.isBlank(pushJobStatusStoreClusterName) && multiRegion) {
      throw new VeniceException(
          ("Unable to send the push job details because " + ConfigKeys.PUSH_JOB_STATUS_STORE_CLUSTER_NAME)
              + " is not configured");
    }
    String pushJobDetailsStoreName = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
    if (pushJobDetailsRTTopic == null) {
      // Verify the RT topic exists and give some time in case it's getting created.
      PubSubTopic expectedRTTopic =
          admin.getPubSubTopicRepository().getTopic(Utils.composeRealTimeTopic(pushJobDetailsStoreName));
      for (int attempt = 0; attempt < VeniceHelixAdmin.INTERNAL_STORE_GET_RRT_TOPIC_ATTEMPTS; attempt++) {
        if (attempt > 0) {
          Utils.sleep(VeniceHelixAdmin.INTERNAL_STORE_RTT_RETRY_BACKOFF_MS);
        }
        if (admin.getTopicManager().containsTopicAndAllPartitionsAreOnline(expectedRTTopic)) {
          pushJobDetailsRTTopic = expectedRTTopic;
          LOGGER.info("Topic {} exists and is configured to receive push job details events", expectedRTTopic);
          break;
        }
      }
      if (pushJobDetailsRTTopic == null) {
        throw new VeniceException(
            "Expected RT topic " + expectedRTTopic + " to receive push job details events"
                + " not found. The topic either hasn't been created yet or it's mis-configured");
      }
    }

    VeniceWriter pushJobDetailsWriter = jobTrackingVeniceWriterMap.computeIfAbsent(PUSH_JOB_DETAILS_WRITER, k -> {
      pushJobDetailsSchemaId = fetchSystemStoreSchemaId(
          pushJobStatusStoreClusterName,
          pushJobDetailsStoreName,
          value.getSchema().toString());
      return admin.getVeniceWriterFactory()
          .createVeniceWriter(
              new VeniceWriterOptions.Builder(pushJobDetailsRTTopic.getName())
                  .setKeyPayloadSerializer(new VeniceAvroKafkaSerializer(key.getSchema().toString()))
                  .setValuePayloadSerializer(new VeniceAvroKafkaSerializer(value.getSchema().toString()))
                  .build());
    });

    pushJobDetailsWriter.put(key, value, pushJobDetailsSchemaId, null);
  }

  PushJobDetails getPushJobDetails(PushJobStatusRecordKey key) {
    Validate.notNull(key);
    ConcurrencyUtils.executeUnderConditionalLock(() -> {
      String storeName = VeniceSystemStoreUtils.getPushJobDetailsStoreName();
      String d2Service = admin.getRouterD2Service(admin.discoverCluster(storeName));
      pushJobDetailsStoreClient = ClientFactory.getAndStartSpecificAvroClient(
          ClientConfig.defaultSpecificClientConfig(storeName, PushJobDetails.class)
              .setD2ServiceName(d2Service)
              .setD2Client(d2Client));
    }, () -> pushJobDetailsStoreClient == null, pushJobDetailsClientLock);
    try {
      return pushJobDetailsStoreClient.get(key).get();
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  BatchJobHeartbeatValue getBatchJobHeartbeatValue(BatchJobHeartbeatKey batchJobHeartbeatKey) {
    Validate.notNull(batchJobHeartbeatKey);
    ConcurrencyUtils.executeUnderConditionalLock(() -> {
      String storeName = VeniceSystemStoreType.BATCH_JOB_HEARTBEAT_STORE.getPrefix();
      String d2Service = admin.getRouterD2Service(admin.discoverCluster(storeName));
      livenessHeartbeatStoreClient = ClientFactory.getAndStartSpecificAvroClient(
          ClientConfig.defaultSpecificClientConfig(storeName, BatchJobHeartbeatValue.class)
              .setD2ServiceName(d2Service)
              .setD2Client(d2Client));
    }, () -> livenessHeartbeatStoreClient == null, livenessHeartbeatClientLock);
    try {
      return livenessHeartbeatStoreClient.get(batchJobHeartbeatKey).get();
    } catch (Exception e) {
      throw new VeniceException(e);
    }
  }

  @VisibleForTesting
  void setPushJobDetailsStoreClient(AvroSpecificStoreClient<PushJobStatusRecordKey, PushJobDetails> client) {
    pushJobDetailsStoreClient = client;
  }

  /**
   * Resolve a value schema id for an internal system store. When this controller is the leader for the
   * hosting cluster the lookup is local; otherwise it falls back to a remote controller call.
   */
  private Integer fetchSystemStoreSchemaId(String clusterName, String storeName, String valueSchemaStr) {
    if (admin.isLeaderControllerFor(clusterName)) {
      int valueSchemaId = admin.getValueSchemaId(clusterName, storeName, valueSchemaStr);
      if (SchemaData.INVALID_VALUE_SCHEMA_ID == valueSchemaId) {
        throw new InvalidVeniceSchemaException(
            "Can not find any registered value schema for the store " + storeName + " that matches the requested schema"
                + valueSchemaStr);
      }
      return valueSchemaId;
    }

    ControllerClient controllerClient = ControllerClient.constructClusterControllerClient(
        clusterName,
        admin.getLeaderController(clusterName).getUrl(sslEnabled),
        sslFactory);
    SchemaResponse response = controllerClient.getValueSchemaID(storeName, valueSchemaStr);
    if (response.isError()) {
      throw new VeniceException(
          "Failed to fetch schema id for store: " + storeName + ", error: " + response.getError());
    }
    return response.getId();
  }

  @Override
  public void close() {
    jobTrackingVeniceWriterMap.values().forEach(Utils::closeQuietlyWithErrorLogged);
    jobTrackingVeniceWriterMap.clear();
    Utils.closeQuietlyWithErrorLogged(pushJobDetailsStoreClient);
    Utils.closeQuietlyWithErrorLogged(livenessHeartbeatStoreClient);
  }
}
