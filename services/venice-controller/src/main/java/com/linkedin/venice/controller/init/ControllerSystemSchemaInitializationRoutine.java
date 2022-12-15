package com.linkedin.venice.controller.init;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceUnsupportedOperationException;
import com.linkedin.venice.init.AbstractSystemSchemaInitializationRoutine;
import com.linkedin.venice.init.AbstractSystemSchemaInitializer;
import com.linkedin.venice.init.ControllerClientBackedSystemSchemaInitializer;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaRepoBackedSchemaReader;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.SchemaPresenceChecker;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


/**
 * A {@link ClusterInitializationRoutine} to orchestrate the initialization of schema system stores by delegating to
 * either:
 * 1. {@link LeaderControllerSystemSchemaInitializer} if the controller transitioned to the LEADER state; or
 * 2. {@link ControllerClientBackedSystemSchemaInitializer} if the controller transitioned to the STANDBY state; or
 * 3. Do nothing (if configs denote it as such)
 */
public class ControllerSystemSchemaInitializationRoutine extends AbstractSystemSchemaInitializationRoutine
    implements ClusterInitializationRoutine, Closeable {
  private final Lazy<ControllerClient> primaryControllerClient;
  private final VeniceHelixAdmin admin;
  private final ReadOnlySchemaRepository schemaRepo;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;

  public ControllerSystemSchemaInitializationRoutine(
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      VeniceHelixAdmin admin,
      ReadOnlySchemaRepository schemaRepo) {
    this.admin = admin;
    this.schemaRepo = schemaRepo;
    this.multiClusterConfigs = multiClusterConfigs;
    Optional<SSLFactory> sslFactory = admin.getSslFactory();
    primaryControllerClient = Lazy.of(
        () -> new D2ControllerClient(
            multiClusterConfigs.getCommonConfig().getD2ServiceName(),
            multiClusterConfigs.getSystemSchemaClusterName(),
            multiClusterConfigs.getZkAddress(),
            sslFactory));
  }

  private AbstractSystemSchemaInitializer getSystemSchemaInitializer(VeniceSystemStoreType systemStore) {
    if (admin.isLeaderControllerFor(multiClusterConfigs.getSystemSchemaClusterName())) {
      return getLeaderControllerSystemSchemaInitializer(systemStore);
    } else if (multiClusterConfigs.getCommonConfig().shouldAutoRegisterWriteSystemSchemas()) {
      return getControllerClientBackedSystemSchemaInitializer(systemStore);
    } else {
      throw new VeniceUnsupportedOperationException("Schema system store initialization for non-leader controllers");
    }
  }

  private AbstractSystemSchemaInitializer getSystemSchemaInitializer(
      VeniceSystemStoreType systemStore,
      UpdateStoreQueryParams updateStoreParams) {
    if (admin.isLeaderControllerFor(multiClusterConfigs.getSystemSchemaClusterName())) {
      return getLeaderControllerSystemSchemaInitializer(systemStore, updateStoreParams);
    } else if (multiClusterConfigs.getCommonConfig().shouldAutoRegisterWriteSystemSchemas()) {
      return getControllerClientBackedSystemSchemaInitializer(systemStore, updateStoreParams);
    } else {
      throw new VeniceUnsupportedOperationException("Schema system store initialization for non-leader controllers");
    }
  }

  private AbstractSystemSchemaInitializer getLeaderControllerSystemSchemaInitializer(
      VeniceSystemStoreType systemStore) {
    return new LeaderControllerSystemSchemaInitializer(systemStore, multiClusterConfigs, admin);
  }

  private AbstractSystemSchemaInitializer getLeaderControllerSystemSchemaInitializer(
      VeniceSystemStoreType systemStore,
      UpdateStoreQueryParams updateStoreParams) {
    return new LeaderControllerSystemSchemaInitializer(
        systemStore,
        multiClusterConfigs,
        admin,
        updateStoreParams,
        true);
  }

  private AbstractSystemSchemaInitializer getControllerClientBackedSystemSchemaInitializer(
      VeniceSystemStoreType systemStore) {
    return new ControllerClientBackedSystemSchemaInitializer(
        primaryControllerClient.get(),
        systemStore,
        new SchemaPresenceChecker(
            new SchemaRepoBackedSchemaReader(schemaRepo, systemStore.getZkSharedStoreName()),
            systemStore.getValueSchemaProtocol()),
        multiClusterConfigs.getCommonConfig().shouldAutoCreateWriteSystemStores());
  }

  private AbstractSystemSchemaInitializer getControllerClientBackedSystemSchemaInitializer(
      VeniceSystemStoreType systemStore,
      UpdateStoreQueryParams updateStoreParams) {
    return new ControllerClientBackedSystemSchemaInitializer(
        primaryControllerClient.get(),
        systemStore,
        new SchemaPresenceChecker(
            new SchemaRepoBackedSchemaReader(schemaRepo, systemStore.getZkSharedStoreName()),
            systemStore.getValueSchemaProtocol()),
        multiClusterConfigs.getCommonConfig().shouldAutoCreateWriteSystemStores(),
        updateStoreParams,
        true);
  }

  @Override
  public void close() {
    primaryControllerClient.ifPresent(Utils::closeQuietlyWithErrorLogged);
  }

  @Override
  public List<AbstractSystemSchemaInitializer> getSystemSchemaInitializers() {
    List<AbstractSystemSchemaInitializer> systemSchemaInitializerList = new ArrayList<>();
    systemSchemaInitializerList.add(getSystemSchemaInitializer(VeniceSystemStoreType.KAFKA_MESSAGE_ENVELOPE));
    systemSchemaInitializerList.add(getSystemSchemaInitializer(VeniceSystemStoreType.PARTITION_STATE));
    systemSchemaInitializerList.add(getSystemSchemaInitializer(VeniceSystemStoreType.STORE_VERSION_STATE));

    if (multiClusterConfigs.isZkSharedMetaSystemSchemaStoreAutoCreationEnabled()) {
      // Add routine to create zk shared metadata system store
      systemSchemaInitializerList.add(
          getSystemSchemaInitializer(
              VeniceSystemStoreType.META_STORE,
              AbstractSystemSchemaInitializer.DEFAULT_USER_SYSTEM_STORE_UPDATE_QUERY_PARAMS));
    }

    if (multiClusterConfigs.isZkSharedDaVinciPushStatusSystemSchemaStoreAutoCreationEnabled()) {
      // Add routine to create zk shared da vinci push status system store
      systemSchemaInitializerList.add(
          getSystemSchemaInitializer(
              VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE,
              AbstractSystemSchemaInitializer.DEFAULT_USER_SYSTEM_STORE_UPDATE_QUERY_PARAMS));
    }
    return systemSchemaInitializerList;
  }

  @Override
  public void execute(String clusterToInit) {
    InitializationState initializationState = this.execute();
    if (initializationState != InitializationState.SUCCEEDED) {
      throw new VeniceException("Schema initialization could not finish successfully. State = " + initializationState);
    }
  }
}
