package com.linkedin.venice.server.init;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.init.AbstractSystemSchemaInitializationRoutine;
import com.linkedin.venice.init.AbstractSystemSchemaInitializer;
import com.linkedin.venice.init.ControllerClientBackedSystemSchemaInitializer;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.schema.SchemaRepoBackedSchemaReader;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.SchemaPresenceChecker;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


public class ServerSystemSchemaInitializationRoutine extends AbstractSystemSchemaInitializationRoutine
    implements AutoCloseable {
  private final Lazy<ControllerClient> primaryControllerClient;
  private final boolean autoRegisterSystemSchemas;
  private final boolean autoCreateMissingSchemaStores;

  private final ReadOnlySchemaRepository schemaRepo;
  private final SchemaReader partitionStateSchemaReader;
  private final SchemaReader storeVersionStateSchemaReader;
  private final SchemaReader kafkaMessageEnvelopeSchemaReader;

  public ServerSystemSchemaInitializationRoutine(
      VeniceServerConfig serverConfig,
      Optional<SSLFactory> sslFactory,
      ReadOnlySchemaRepository schemaRepo,
      SchemaReader partitionStateSchemaReader,
      SchemaReader storeVersionStateSchemaReader,
      SchemaReader kafkaMessageEnvelopeSchemaReader) {
    primaryControllerClient = Lazy.of(
        () -> new D2ControllerClient(
            serverConfig.getZookeeperAddress(),
            serverConfig.getSystemSchemaClusterName(),
            serverConfig.getLocalControllerD2ServiceName(),
            sslFactory));
    autoRegisterSystemSchemas = serverConfig.shouldAutoRegisterWriteSystemSchemas();
    autoCreateMissingSchemaStores = serverConfig.shouldAutoCreateWriteSystemStores();
    this.schemaRepo = schemaRepo;
    this.partitionStateSchemaReader = partitionStateSchemaReader;
    this.storeVersionStateSchemaReader = storeVersionStateSchemaReader;
    this.kafkaMessageEnvelopeSchemaReader = kafkaMessageEnvelopeSchemaReader;
  }

  private Optional<AbstractSystemSchemaInitializer> getSystemSchemaInitializer(
      VeniceSystemStoreType schemaStore,
      SchemaReader systemStoreSchemaReader,
      boolean autoRegisterSystemSchemas) {
    return getSystemSchemaInitializer(schemaStore, systemStoreSchemaReader, autoRegisterSystemSchemas, null, false);
  }

  private Optional<AbstractSystemSchemaInitializer> getSystemSchemaInitializer(
      VeniceSystemStoreType schemaStore,
      SchemaReader systemStoreSchemaReader,
      boolean autoRegisterSystemSchemas,
      UpdateStoreQueryParams updateStoreParams) {
    return getSystemSchemaInitializer(
        schemaStore,
        systemStoreSchemaReader,
        autoRegisterSystemSchemas,
        updateStoreParams,
        true);
  }

  private Optional<AbstractSystemSchemaInitializer> getSystemSchemaInitializer(
      VeniceSystemStoreType schemaStore,
      SchemaReader systemStoreSchemaReader,
      boolean autoRegisterSystemSchemas,
      UpdateStoreQueryParams updateStoreParams,
      boolean autoRegisterDerivedComputeSchema) {
    SchemaPresenceChecker schemaPresenceChecker =
        new SchemaPresenceChecker(systemStoreSchemaReader, schemaStore.getValueSchemaProtocol());

    if (autoRegisterSystemSchemas) {
      return Optional.of(
          new ControllerClientBackedSystemSchemaInitializer(
              primaryControllerClient.get(),
              schemaStore,
              schemaPresenceChecker,
              autoCreateMissingSchemaStores,
              updateStoreParams,
              autoRegisterDerivedComputeSchema));
    } else {
      // verify the current version of the system schemas are registered in ZK before moving ahead
      schemaPresenceChecker.verifySchemaVersionPresentOrExit();
      return Optional.empty();
    }
  }

  @Override
  public List<AbstractSystemSchemaInitializer> getSystemSchemaInitializers() {
    List<AbstractSystemSchemaInitializer> systemSchemaInitializerList = new ArrayList<>();
    getSystemSchemaInitializer(
        VeniceSystemStoreType.PARTITION_STATE,
        partitionStateSchemaReader,
        autoRegisterSystemSchemas).ifPresent(systemSchemaInitializerList::add);
    getSystemSchemaInitializer(
        VeniceSystemStoreType.STORE_VERSION_STATE,
        storeVersionStateSchemaReader,
        autoRegisterSystemSchemas).ifPresent(systemSchemaInitializerList::add);
    getSystemSchemaInitializer(
        VeniceSystemStoreType.KAFKA_MESSAGE_ENVELOPE,
        kafkaMessageEnvelopeSchemaReader,
        autoRegisterSystemSchemas).ifPresent(systemSchemaInitializerList::add);

    getSystemSchemaInitializer(
        VeniceSystemStoreType.META_STORE,
        new SchemaRepoBackedSchemaReader(schemaRepo, VeniceSystemStoreType.META_STORE.getZkSharedStoreName()),
        autoRegisterSystemSchemas,
        AbstractSystemSchemaInitializer.DEFAULT_USER_SYSTEM_STORE_UPDATE_QUERY_PARAMS)
            .ifPresent(systemSchemaInitializerList::add);

    getSystemSchemaInitializer(
        VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE,
        new SchemaRepoBackedSchemaReader(
            schemaRepo,
            VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE.getZkSharedStoreName()),
        autoRegisterSystemSchemas,
        AbstractSystemSchemaInitializer.DEFAULT_USER_SYSTEM_STORE_UPDATE_QUERY_PARAMS)
            .ifPresent(systemSchemaInitializerList::add);

    return systemSchemaInitializerList;
  }

  @Override
  public void close() throws Exception {
    primaryControllerClient.ifPresent(Utils::closeQuietlyWithErrorLogged);
  }
}
