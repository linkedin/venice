package com.linkedin.venice.controller.init;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.init.ControllerClientBackedSystemSchemaInitializer;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaRepoBackedSchemaReader;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.SchemaPresenceChecker;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;


/**
 * A {@link ClusterInitializationRoutine} to orchestrate the initialization of schema system stores by delegating to
 * either:
 * 1. {@link LeaderControllerSystemSchemaInitializer} if the controller transitioned to the LEADER state; or
 * 2. {@link ControllerClientBackedSystemSchemaInitializer} if the controller transitioned to the STANDBY state; or
 * 3. Do nothing (if configs denote it as such)
 */
public class ControllerSystemSchemaInitializer implements ClusterInitializationRoutine, Closeable {
  private final Lazy<ControllerClient> parentControllerClient;
  private final Lazy<D2Client> parentColoD2Client;
  private final VeniceHelixAdmin admin;
  private final ReadOnlySchemaRepository schemaRepo;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;

  public ControllerSystemSchemaInitializer(
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      VeniceHelixAdmin admin,
      ReadOnlySchemaRepository schemaRepo) {
    this.admin = admin;
    this.schemaRepo = schemaRepo;
    this.multiClusterConfigs = multiClusterConfigs;
    Optional<SSLFactory> sslFactory = admin.getSslFactory();
    parentColoD2Client = Lazy.of(() -> {
      D2Client d2Client =
          new D2ClientBuilder().setZkHosts(multiClusterConfigs.getCommonConfig().getPrimaryControllerD2ZkHosts())
              .setSSLContext(sslFactory.map(SSLFactory::getSSLContext).orElse(null))
              .setIsSSLEnabled(sslFactory.isPresent())
              .setSSLParameters(sslFactory.map(SSLFactory::getSSLParameters).orElse(null))
              .setFsBasePath(Utils.getUniqueTempPath("d2"))
              .setEnableSaveUriDataOnDisk(true)
              .build();

      D2ClientUtils.startClient(d2Client);
      return d2Client;
    });
    parentControllerClient = Lazy.of(
        () -> new D2ControllerClient(
            multiClusterConfigs.getCommonConfig().getPrimaryControllerD2ServiceName(),
            multiClusterConfigs.getSystemSchemaClusterName(),
            parentColoD2Client.get(),
            sslFactory));

  }

  private void registerSystemSchema(VeniceSystemStoreType systemStore) {
    if (admin.isLeaderControllerFor(multiClusterConfigs.getSystemSchemaClusterName())) {
      registerSystemSchemaAsLeaderController(systemStore);
    } else if (multiClusterConfigs.getCommonConfig().shouldAutoRegisterWriteSystemSchemas()) {
      registerSystemSchemaUsingControllerClient(systemStore);
    } else {
      throw new VeniceUnsupportedOperationException("Schema system store initialization for non-leader controllers");
    }
  }

  private void registerSystemSchema(VeniceSystemStoreType systemStore, UpdateStoreQueryParams updateStoreParams) {
    if (admin.isLeaderControllerFor(multiClusterConfigs.getSystemSchemaClusterName())) {
      registerSystemSchemaAsLeaderController(systemStore, updateStoreParams);
    } else if (multiClusterConfigs.getCommonConfig().shouldAutoRegisterWriteSystemSchemas()) {
      registerSystemSchemaUsingControllerClient(systemStore, updateStoreParams);
    } else {
      throw new VeniceUnsupportedOperationException("Schema system store initialization for non-leader controllers");
    }
  }

  private void registerSystemSchemaAsLeaderController(VeniceSystemStoreType systemStore) {
    new LeaderControllerSystemSchemaInitializer(systemStore, multiClusterConfigs, admin).execute();
  }

  private void registerSystemSchemaAsLeaderController(
      VeniceSystemStoreType systemStore,
      UpdateStoreQueryParams updateStoreParams) {
    new LeaderControllerSystemSchemaInitializer(systemStore, multiClusterConfigs, admin, updateStoreParams, true)
        .execute();
  }

  private void registerSystemSchemaUsingControllerClient(VeniceSystemStoreType systemStore) {
    new ControllerClientBackedSystemSchemaInitializer(
        parentControllerClient.get(),
        systemStore,
        new SchemaPresenceChecker(
            new SchemaRepoBackedSchemaReader(schemaRepo, systemStore.getZkSharedStoreName()),
            systemStore.getValueSchemaProtocol()),
        multiClusterConfigs.getCommonConfig().shouldAutoCreateWriteSystemStores()).execute();
  }

  private void registerSystemSchemaUsingControllerClient(
      VeniceSystemStoreType systemStore,
      UpdateStoreQueryParams updateStoreParams) {
    new ControllerClientBackedSystemSchemaInitializer(
        parentControllerClient.get(),
        systemStore,
        new SchemaPresenceChecker(
            new SchemaRepoBackedSchemaReader(schemaRepo, systemStore.getZkSharedStoreName()),
            systemStore.getValueSchemaProtocol()),
        multiClusterConfigs.getCommonConfig().shouldAutoCreateWriteSystemStores(),
        updateStoreParams,
        true).execute();
  }

  @Override
  public void close() {
    parentControllerClient.ifPresent(Utils::closeQuietlyWithErrorLogged);
    parentColoD2Client.ifPresent(D2ClientUtils::shutdownClient);
  }

  @Override
  public void execute(String clusterToInit) {
    registerSystemSchema(VeniceSystemStoreType.KAFKA_MESSAGE_ENVELOPE);
    registerSystemSchema(VeniceSystemStoreType.PARTITION_STATE);
    registerSystemSchema(VeniceSystemStoreType.STORE_VERSION_STATE);

    if (multiClusterConfigs.isZkSharedMetaSystemSchemaStoreAutoCreationEnabled()) {
      // Add routine to create zk shared metadata system store
      UpdateStoreQueryParams metadataSystemStoreUpdate =
          new UpdateStoreQueryParams().setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(1)) // 1 day rewind
              .setHybridOffsetLagThreshold(1)
              .setHybridTimeLagThreshold(TimeUnit.MINUTES.toSeconds(1)) // 1 mins
              .setLeaderFollowerModel(true)
              .setWriteComputationEnabled(true)
              .setPartitionCount(1);
      registerSystemSchema(VeniceSystemStoreType.META_STORE, metadataSystemStoreUpdate);
    }

    if (multiClusterConfigs.isZkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled()) {
      // Add routine to create zk shared da vinci push status system store
      UpdateStoreQueryParams daVinciPushStatusSystemStoreUpdate =
          new UpdateStoreQueryParams().setHybridRewindSeconds(TimeUnit.DAYS.toSeconds(1)) // 1 day rewind
              .setHybridOffsetLagThreshold(1)
              .setHybridTimeLagThreshold(TimeUnit.MINUTES.toSeconds(1)) // 1 mins
              .setLeaderFollowerModel(true)
              .setWriteComputationEnabled(true)
              .setPartitionCount(1);
      registerSystemSchema(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE, daVinciPushStatusSystemStoreUpdate);
    }
  }
}
