package com.linkedin.venice.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import java.util.Optional;
import java.util.Set;


public class VeniceControllerApiHandler {
  private final int port;
  private final Admin admin;
  private final boolean enforceSSL;
  private final boolean sslEnabled;
  private final boolean checkReadMethodForKafka;
  private final Optional<SSLConfig> sslConfig;
  private final Optional<DynamicAccessController> accessController;

  protected static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static String REQUEST_START_TIME = "startTime";
  private static String REQUEST_SUCCEED = "succeed";

  private final List<ControllerRoute> disabledRoutes;

  private final boolean disableParentRequestTopicForStreamPushes;
  private final PubSubTopicRepository pubSubTopicRepository;

  // Use this for access controls and other security related checks
  VeniceControllerAccessControlService veniceControllerAccessControlService;

  public VeniceControllerApiHandler(
      int port,
      Admin admin,
      MetricsRepository metricsRepository,
      Set<String> clusters,
      boolean enforceSSL,
      Optional<SSLConfig> sslConfig,
      boolean checkReadMethodForKafka,
      Optional<DynamicAccessController> accessController,
      List<ControllerRoute> disabledRoutes,
      VeniceProperties veniceProperties,
      boolean disableParentRequestTopicForStreamPushes,
      PubSubTopicRepository pubSubTopicRepository) {
    this.port = port;
    this.enforceSSL = enforceSSL;
    this.sslEnabled = sslConfig.isPresent();
    this.sslConfig = sslConfig;
    this.checkReadMethodForKafka = checkReadMethodForKafka;
    this.accessController = accessController;
    // Note: admin is passed in as a reference. The expectation is the source of the admin will
    // close it so we don't close it in stopInner()
    this.admin = admin;
    this.disabledRoutes = disabledRoutes;
    this.disableParentRequestTopicForStreamPushes = disableParentRequestTopicForStreamPushes;
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.veniceControllerAccessControlService = new VeniceControllerAccessControlService(sslEnabled, accessController);
  }
}
