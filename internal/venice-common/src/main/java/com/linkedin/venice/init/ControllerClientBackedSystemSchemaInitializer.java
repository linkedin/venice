package com.linkedin.venice.init;

import static com.linkedin.venice.exceptions.ErrorType.STORE_NOT_FOUND;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.SchemaPresenceChecker;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import org.apache.avro.Schema;


/**
 * A {@link AbstractSystemSchemaInitializer} impl to perform the actions necessary to register schema system stores by
 * invoking the necessary Controller APIs via the {@link ControllerClient}.
 */
public class ControllerClientBackedSystemSchemaInitializer extends AbstractSystemSchemaInitializer {
  private final ControllerClient controllerClient;
  private final VeniceSystemStoreType systemStore;
  private final SchemaPresenceChecker schemaPresenceChecker;
  private final boolean autoCreateMissingSchemaStores;

  public ControllerClientBackedSystemSchemaInitializer(
      ControllerClient controllerClient,
      VeniceSystemStoreType systemStore,
      SchemaPresenceChecker schemaPresenceChecker,
      boolean autoCreateMissingSchemaStores) {
    this(controllerClient, systemStore, schemaPresenceChecker, autoCreateMissingSchemaStores, null, false);
  }

  public ControllerClientBackedSystemSchemaInitializer(
      ControllerClient controllerClient,
      VeniceSystemStoreType systemStore,
      SchemaPresenceChecker schemaPresenceChecker,
      boolean autoCreateMissingSchemaStores,
      UpdateStoreQueryParams updateStoreQueryParams,
      boolean autoRegisterDerivedComputeSchema) {
    super(systemStore, controllerClient.getClusterName(), updateStoreQueryParams, autoRegisterDerivedComputeSchema);
    this.controllerClient = controllerClient;
    this.systemStore = systemStore;
    this.schemaPresenceChecker = schemaPresenceChecker;
    this.autoCreateMissingSchemaStores = autoCreateMissingSchemaStores;
  }

  @Override
  protected String getActualSystemStoreCluster() {
    D2ServiceDiscoveryResponse response =
        controllerClient.retryableRequest(10, cc -> cc.discoverCluster(getSchemaStoreName()));
    if (response.isError()) {
      if (STORE_NOT_FOUND.equals(response.getErrorType())) {
        return null;
      } else {
        throw new VeniceException(response.getError());
      }
    }

    return response.getCluster();
  }

  @Override
  protected boolean doesStoreExist() {
    StoreResponse response = controllerClient.retryableRequest(10, cc -> cc.getStore(getSchemaStoreName()));
    if (response.isError()) {
      if (STORE_NOT_FOUND.equals(response.getErrorType())) {
        return false;
      } else {
        throw new VeniceException(response.getError());
      }
    }

    return true;
  }

  @Override
  protected boolean shouldCreateMissingSchemaStores() {
    return autoCreateMissingSchemaStores;
  }

  @Override
  protected void createSchemaStore(Schema firstKeySchema, Schema firstValueSchema) {
    NewStoreResponse newStoreResponse = controllerClient.retryableRequest(
        10,
        cc -> cc.createNewSystemStore(
            systemStore.getZkSharedStoreName(),
            VeniceConstants.SYSTEM_STORE_OWNER,
            firstKeySchema.toString(),
            firstValueSchema.toString()));

    if (newStoreResponse.isError()) {
      throw new VeniceException(newStoreResponse.getError());
    }
  }

  @Override
  protected void updateStore(UpdateStoreQueryParams updateStoreQueryParams) {
    ControllerResponse response =
        controllerClient.retryableRequest(10, cc -> cc.updateStore(getSchemaStoreName(), updateStoreQueryParams));

    if (response.isError()) {
      throw new VeniceException(response.getError());
    }
  }

  @Override
  protected SchemaEntry getRegisteredKeySchema() {
    SchemaResponse response = controllerClient.retryableRequest(10, cc -> cc.getKeySchema(getSchemaStoreName()));

    if (response.isError()) {
      throw new VeniceException(response.getError());
    }

    return new SchemaEntry(response.getId(), AvroCompatibilityHelper.parse(response.getSchemaStr()));
  }

  @Override
  protected Collection<SchemaEntry> getAllRegisteredValueSchemas() {
    MultiSchemaResponse response =
        controllerClient.retryableRequest(10, cc -> cc.getAllValueSchema(getSchemaStoreName()));

    if (response.isError()) {
      throw new VeniceException(response.getError());
    }

    return Arrays.stream(response.getSchemas())
        .map(schema -> new SchemaEntry(schema.getId(), AvroCompatibilityHelper.parse(schema.getSchemaStr())))
        .collect(Collectors.toSet());
  }

  @Override
  protected void registerValueSchema(Schema schema, int version) {
    SchemaResponse response = controllerClient
        .retryableRequest(10, cc -> cc.addValueSchema(getSchemaStoreName(), schema.toString(), version));

    if (response.isError()) {
      throw new VeniceException(response.getError());
    }
  }

  @Override
  protected ValueAndDerivedSchemaId getDerivedSchemaInfo(Schema derivedSchema) {
    SchemaResponse response = controllerClient
        .retryableRequest(10, cc -> cc.getValueOrDerivedSchemaId(getSchemaStoreName(), derivedSchema.toString()));

    if (response.isError()) {
      throw new VeniceException(response.getError());
    }

    return new ValueAndDerivedSchemaId(response.getId(), response.getDerivedSchemaId());
  }

  @Override
  protected void registerDerivedSchema(int valueSchemaVersion, Schema writeComputeSchema) {
    SchemaResponse response = controllerClient.retryableRequest(
        10,
        cc -> cc.addDerivedSchema(getSchemaStoreName(), valueSchemaVersion, writeComputeSchema.toString()));

    if (response.isError()) {
      throw new VeniceException(response.getError());
    }
  }

  @Override
  public boolean isSchemaInitialized() {
    return schemaPresenceChecker
        .isSchemaVersionPresent(systemStore.getValueSchemaProtocol().getCurrentProtocolVersion(), false);
  }
}
