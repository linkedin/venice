package com.linkedin.venice.controller;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.controller.lingeringjob.DefaultLingeringStoreVersionChecker;
import com.linkedin.venice.controller.lingeringjob.HeartbeatBasedCheckerStats;
import com.linkedin.venice.controller.lingeringjob.HeartbeatBasedLingeringStoreVersionChecker;
import com.linkedin.venice.controller.lingeringjob.LingeringStoreVersionChecker;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.service.ICProvider;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.linkedin.venice.client.store.ClientFactory.*;


/**
 * A service venice controller. Wraps Helix Controller.
 */
public class VeniceControllerService extends AbstractVeniceService {
  private static final Logger logger = LogManager.getLogger(VeniceControllerService.class);

  private final Admin admin;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final Map<String, AdminConsumerService> consumerServicesByClusters;

  public VeniceControllerService(VeniceControllerMultiClusterConfig multiClusterConfigs,
      MetricsRepository metricsRepository, boolean sslEnabled, Optional<SSLConfig> sslConfig,
      Optional<DynamicAccessController> accessController, Optional<AuthorizerService> authorizerService, D2Client d2Client,
      Optional<ClientConfig> routerClientConfig, Optional<ICProvider> icProvider) {

    this.multiClusterConfigs = multiClusterConfigs;
    VeniceHelixAdmin internalAdmin = new VeniceHelixAdmin(multiClusterConfigs, metricsRepository, sslEnabled, d2Client,
        sslConfig, accessController, icProvider);

    if (multiClusterConfigs.isParent()) {
      this.admin = new VeniceParentHelixAdmin(internalAdmin, multiClusterConfigs, sslEnabled, sslConfig, accessController,
          authorizerService, createLingeringStoreVersionChecker(multiClusterConfigs, metricsRepository));
      logger.info("Controller works as a parent controller.");
    } else {
      this.admin = internalAdmin;
      logger.info("Controller works as a normal controller.");
    }
    Optional<SchemaReader> kafkaMessageEnvelopeSchemaReader = Optional.empty();
    try {
      kafkaMessageEnvelopeSchemaReader = routerClientConfig.isPresent() ? Optional.of(
          getSchemaReader(ClientConfig.cloneConfig(routerClientConfig.get()).setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName()))) : Optional.empty();
    } catch (Exception e) {
      logger.error("Exception in initializing KME schema reader", e);
    }
    // The admin consumer needs to use VeniceHelixAdmin to update Zookeeper directly
    consumerServicesByClusters = new HashMap<>(multiClusterConfigs.getClusters().size());
    for (String cluster : multiClusterConfigs.getClusters()) {
      AdminConsumerService adminConsumerService =
          new AdminConsumerService(cluster, internalAdmin, multiClusterConfigs.getControllerConfig(cluster), metricsRepository, kafkaMessageEnvelopeSchemaReader);
      this.consumerServicesByClusters.put(cluster, adminConsumerService);

      this.admin.setAdminConsumerService(cluster, adminConsumerService);
    }
  }

  private LingeringStoreVersionChecker createLingeringStoreVersionChecker(
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      MetricsRepository metricsRepository
  ) {
    if (multiClusterConfigs.getBatchJobHeartbeatEnabled()) {
      logger.info("Batch job heartbeat is enabled. Hence use the heartbeat-based batch job liveness checker.");
      return new HeartbeatBasedLingeringStoreVersionChecker(
          multiClusterConfigs.getBatchJobHeartbeatTimeout(),
          multiClusterConfigs.getBatchJobHeartbeatInitialBufferTime(),
          new DefaultLingeringStoreVersionChecker(),
          new HeartbeatBasedCheckerStats(metricsRepository)
      );
    } {
      logger.info("Batch job heartbeat is NOT enabled. Hence use the default batch job liveness checker.");
      return new DefaultLingeringStoreVersionChecker();
    }
  }

  @Override
  public boolean startInner() {
    for (String clusterName : multiClusterConfigs.getClusters()) {
      admin.initStorageCluster(clusterName);
      consumerServicesByClusters.get(clusterName).start();
      logger.info("started cluster: " + clusterName);
    }
    logger.info("Started Venice controller.");
    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() {
    for (String clusterName : multiClusterConfigs.getClusters()) {
      // We don't need to lock resources here, as we will acquire the lock during the ST leader->standby, which would
      // prevent the partial updates.
      admin.stop(clusterName);
      try {
        consumerServicesByClusters.get(clusterName).stop();
      } catch (Exception e) {
        logger.error("Got exception when stop AdminConsumerService", e);
      }
      logger.info("Stopped cluster: "+clusterName);
    }
    // TODO merge or differentiate the difference between close() and stopVeniceController() explicitly.
    admin.stopVeniceController();
    admin.close();

    logger.info("Stopped Venice controller.");
  }

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
