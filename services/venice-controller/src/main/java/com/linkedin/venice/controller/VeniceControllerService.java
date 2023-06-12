package com.linkedin.venice.controller;

import static com.linkedin.venice.client.store.ClientFactory.getSchemaReader;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.lingeringjob.DefaultLingeringStoreVersionChecker;
import com.linkedin.venice.controller.lingeringjob.HeartbeatBasedCheckerStats;
import com.linkedin.venice.controller.lingeringjob.HeartbeatBasedLingeringStoreVersionChecker;
import com.linkedin.venice.controller.lingeringjob.LingeringStoreVersionChecker;
import com.linkedin.venice.controller.supersetschema.SupersetSchemaGenerator;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubClientsFactory;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.KafkaValueSerializer;
import com.linkedin.venice.serialization.avro.OptimizedKafkaValueSerializer;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.pools.LandFillObjectPool;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A service venice controller. Wraps Helix Controller.
 */
public class VeniceControllerService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerService.class);

  private final Admin admin;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final Map<String, AdminConsumerService> consumerServicesByClusters;

  public VeniceControllerService(
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      MetricsRepository metricsRepository,
      boolean sslEnabled,
      Optional<SSLConfig> sslConfig,
      Optional<DynamicAccessController> accessController,
      Optional<AuthorizerService> authorizerService,
      D2Client d2Client,
      Optional<ClientConfig> routerClientConfig,
      Optional<ICProvider> icProvider,
      Optional<SupersetSchemaGenerator> externalSupersetSchemaGenerator,
      PubSubTopicRepository pubSubTopicRepository,
      PubSubClientsFactory pubSubClientsFactory) {
    this.multiClusterConfigs = multiClusterConfigs;
    VeniceHelixAdmin internalAdmin = new VeniceHelixAdmin(
        multiClusterConfigs,
        metricsRepository,
        sslEnabled,
        d2Client,
        sslConfig,
        accessController,
        icProvider,
        pubSubTopicRepository,
        pubSubClientsFactory);

    if (multiClusterConfigs.isParent()) {
      this.admin = new VeniceParentHelixAdmin(
          internalAdmin,
          multiClusterConfigs,
          sslEnabled,
          sslConfig,
          accessController,
          authorizerService,
          createLingeringStoreVersionChecker(multiClusterConfigs, metricsRepository),
          WriteComputeSchemaConverter.getInstance(),
          externalSupersetSchemaGenerator,
          pubSubTopicRepository);
      LOGGER.info("Controller works as a parent controller.");
    } else {
      this.admin = internalAdmin;
      LOGGER.info("Controller works as a child controller.");
    }
    Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader = Optional.empty();
    try {
      kafkaMessageEnvelopeSchemaReader = routerClientConfig.isPresent()
          ? Optional.of(
              getSchemaReader(
                  ClientConfig.cloneConfig(routerClientConfig.get())
                      .setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName()),
                  null))
          : Optional.empty();
    } catch (Exception e) {
      LOGGER.error("Exception in initializing KME schema reader", e);
    }
    // The admin consumer needs to use VeniceHelixAdmin to update Zookeeper directly
    consumerServicesByClusters = new HashMap<>(multiClusterConfigs.getClusters().size());
    /** N.B. The code below is copied from {@link com.linkedin.venice.controller.init.SystemSchemaInitializationRoutine */
    // BiConsumer<Integer, Schema> newSchemaEncountered = (schemaId, schema) -> internalAdmin.addValueSchema(
    // "?", // TODO: Figure out a clean way to retrieve the cluster name param
    // AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(),
    // schema.toString(),
    // schemaId,
    // DirectionalSchemaCompatibilityType.NONE,
    // false);
    KafkaValueSerializer kafkaValueSerializer = new OptimizedKafkaValueSerializer(
    // newSchemaEncountered // TODO: Wire in this hook once we figure out a clean way to do it
    );
    kafkaMessageEnvelopeSchemaReader.ifPresent(kafkaValueSerializer::setSchemaReader);
    PubSubMessageDeserializer pubSubMessageDeserializer = new PubSubMessageDeserializer(
        kafkaValueSerializer,
        new LandFillObjectPool<>(KafkaMessageEnvelope::new),
        new LandFillObjectPool<>(KafkaMessageEnvelope::new));
    for (String cluster: multiClusterConfigs.getClusters()) {
      AdminConsumerService adminConsumerService = new AdminConsumerService(
          internalAdmin,
          multiClusterConfigs.getControllerConfig(cluster),
          metricsRepository,
          pubSubTopicRepository,
          pubSubMessageDeserializer);
      this.consumerServicesByClusters.put(cluster, adminConsumerService);

      this.admin.setAdminConsumerService(cluster, adminConsumerService);
    }

  }

  private LingeringStoreVersionChecker createLingeringStoreVersionChecker(
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      MetricsRepository metricsRepository) {
    if (multiClusterConfigs.getBatchJobHeartbeatEnabled()) {
      LOGGER.info("Batch job heartbeat is enabled. Hence use the heartbeat-based batch job liveness checker.");
      return new HeartbeatBasedLingeringStoreVersionChecker(
          multiClusterConfigs.getBatchJobHeartbeatTimeout(),
          multiClusterConfigs.getBatchJobHeartbeatInitialBufferTime(),
          new DefaultLingeringStoreVersionChecker(),
          new HeartbeatBasedCheckerStats(metricsRepository));
    }
    {
      LOGGER.info("Batch job heartbeat is NOT enabled. Hence use the default batch job liveness checker.");
      return new DefaultLingeringStoreVersionChecker();
    }
  }

  /**
   * Causes {@code VeniceControllerService} to begin execution.
   */
  @Override
  public boolean startInner() {
    for (String clusterName: multiClusterConfigs.getClusters()) {
      admin.initStorageCluster(clusterName);
      consumerServicesByClusters.get(clusterName).start();
      LOGGER.info("started cluster: {}", clusterName);
    }
    LOGGER.info("Started Venice controller.");
    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  /**
   * Causes {@code VeniceControllerService} to stop executing.
   */
  @Override
  public void stopInner() {
    for (String clusterName: multiClusterConfigs.getClusters()) {
      // We don't need to lock resources here, as we will acquire the lock during the ST leader->standby, which would
      // prevent the partial updates.
      admin.stop(clusterName);
      try {
        consumerServicesByClusters.get(clusterName).stop();
      } catch (Exception e) {
        LOGGER.error("Got exception when stop AdminConsumerService", e);
      }
      LOGGER.info("Stopped cluster: {}", clusterName);
    }
    // TODO merge or differentiate the difference between close() and stopVeniceController() explicitly.
    admin.stopVeniceController();
    admin.close();

    LOGGER.info("Stopped Venice controller.");
  }

  /**
   * @return a reference to the HelixAdmin object.
   */
  public Admin getVeniceHelixAdmin() {
    return admin;
  }

  /**
   * This method is for testing.
   * @param cluster
   * @return the admin consumer service for the cluster
   */
  public AdminConsumerService getAdminConsumerServiceByCluster(String cluster) {
    return consumerServicesByClusters.get(cluster);
  }
}
