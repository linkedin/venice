package com.linkedin.venice.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.controllerapi.ControllerRoute;
import com.linkedin.venice.protocols.CreateStoreGrpcRequest;
import com.linkedin.venice.protocols.CreateStoreGrpcResponse;
import com.linkedin.venice.protocols.VeniceControllerGrpcServiceGrpc;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.VeniceProperties;
import io.grpc.stub.StreamObserver;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class VeniceControllerGrpcServiceImpl
    extends VeniceControllerGrpcServiceGrpc.VeniceControllerGrpcServiceImplBase {
  private static final Logger LOGGER = LogManager.getLogger(VeniceControllerGrpcServiceImpl.class);

  private final Admin admin;
  private final int port;
  private final boolean enforceSSL;
  private final boolean sslEnabled;
  private final boolean checkReadMethodForKafka;
  private final Optional<SSLConfig> sslConfig;
  private final Optional<DynamicAccessController> accessController;
  private final List<ControllerRoute> disabledRoutes;
  private final boolean disableParentRequestTopicForStreamPushes;
  private final PubSubTopicRepository pubSubTopicRepository;
  private final MetricsRepository metricsRepository;
  private final VeniceProperties veniceProperties;

  protected static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static String REQUEST_START_TIME = "startTime";
  private static String REQUEST_SUCCEED = "succeed";

  public VeniceControllerGrpcServiceImpl(
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
    this.admin = admin;
    this.enforceSSL = enforceSSL;
    this.sslConfig = sslConfig;
    this.checkReadMethodForKafka = checkReadMethodForKafka;
    this.accessController = accessController;
    this.disabledRoutes = disabledRoutes;
    this.disableParentRequestTopicForStreamPushes = disableParentRequestTopicForStreamPushes;
    this.pubSubTopicRepository = pubSubTopicRepository;
    this.metricsRepository = metricsRepository;
    this.veniceProperties = veniceProperties;
    this.sslEnabled = sslConfig.isPresent();
    LOGGER.info("VeniceControllerGrpcServiceImpl created with enforceSSL: {}, sslEnabled: {}", enforceSSL, sslEnabled);
  }

  public void createStore(CreateStoreGrpcRequest request, StreamObserver<CreateStoreGrpcResponse> responseObserver) {
    String clusterName = request.getClusterName();
    String storeName = request.getStoreName();
    String keySchema = request.getKeySchema();
    String valueSchema = request.getValueSchema();
    boolean isSystemStore = request.getIsSystemStore();
    String owner = request.getOwner();
    Optional<String> accessPermissions = Optional.empty();
    if (request.hasAccessPermission()) {
      accessPermissions = Optional.of(request.getAccessPermission());
    }
    CreateStoreGrpcResponse.Builder responseBuilder = CreateStoreGrpcResponse.newBuilder();
    try {
      admin.createStore(clusterName, storeName, owner, keySchema, valueSchema, isSystemStore, accessPermissions);
      responseBuilder.setStatusCode(200);
    } catch (Exception e) {
      LOGGER.error("Error creating store", e);
      responseBuilder.setStatusCode(500);
      responseBuilder.setStatusMessage("Error creating store: " + e.getMessage());
    }
    responseObserver.onNext(responseBuilder.build());
    responseObserver.onCompleted();
  }
}
