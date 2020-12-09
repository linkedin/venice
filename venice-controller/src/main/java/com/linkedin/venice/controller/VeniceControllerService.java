package com.linkedin.venice.controller;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.authorization.AuthorizerService;
import com.linkedin.venice.controller.kafka.consumer.AdminConsumerService;
import com.linkedin.venice.service.AbstractVeniceService;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.log4j.Logger;


/**
 * A service venice controller. Wraps Helix Controller.
 */
public class VeniceControllerService extends AbstractVeniceService {
  private static final Logger logger = Logger.getLogger(VeniceControllerService.class);

  private final Admin admin;
  private final VeniceControllerMultiClusterConfig mutliClusterConfigs;
  private final Map<String, AdminConsumerService> consumerServices;

  public VeniceControllerService(VeniceControllerMultiClusterConfig multiClusterConfigs,
      MetricsRepository metricsRepository, boolean sslEnabled, Optional<SSLConfig> sslConfig,
      Optional<DynamicAccessController> accessController, Optional<AuthorizerService> authorizerService, Optional<D2Client> d2Client) {
    this.mutliClusterConfigs = multiClusterConfigs;
    VeniceHelixAdmin internalAdmin = new VeniceHelixAdmin(multiClusterConfigs, metricsRepository, sslEnabled, sslConfig, accessController, d2Client);
    if (multiClusterConfigs.isParent()) {
      this.admin = new VeniceParentHelixAdmin(internalAdmin, multiClusterConfigs, sslEnabled, sslConfig, authorizerService);
      logger.info("Controller works as a parent controller.");
    } else {
      this.admin = internalAdmin;
      logger.info("Controller works as a normal controller.");
    }
    // The admin consumer needs to use VeniceHelixAdmin to update Zookeeper directly
    consumerServices = new HashMap<>();
    for (String cluster : multiClusterConfigs.getClusters()) {
      AdminConsumerService adminConsumerService =
          new AdminConsumerService(internalAdmin, multiClusterConfigs.getConfigForCluster(cluster), metricsRepository);
      this.consumerServices.put(cluster, adminConsumerService);

      this.admin.setAdminConsumerService(cluster, adminConsumerService);
    }
  }

  @Override
  public boolean startInner() {
    for (String clusterName : mutliClusterConfigs.getClusters()) {
      admin.start(clusterName);
      consumerServices.get(clusterName).start();
      logger.info("started cluster: " + clusterName);
    }

    logger.info("Started Venice controller.");

    // There is no async process in this function, so we are completely finished with the start up process.
    return true;
  }

  @Override
  public void stopInner() {
    for (String clusterName : mutliClusterConfigs.getClusters()) {
      // We don't need to lock resources here, as we will acquire the lock during the ST leader->standby, which would
      // prevent the partial updates.
      admin.stop(clusterName);
      try {
        consumerServices.get(clusterName).stop();
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
    return consumerServices.get(cluster);
  }
}
