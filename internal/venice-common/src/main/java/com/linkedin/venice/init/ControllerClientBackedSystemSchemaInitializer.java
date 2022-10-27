package com.linkedin.venice.init;

import static com.linkedin.venice.exceptions.ErrorType.STORE_NOT_FOUND;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.ControllerClient;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A {@link AbstractSystemSchemaInitializer} impl to perform the actions necessary to register schema system stores by
 * invoking the necessary Controller APIs via the {@link ControllerClient}.
 */
public class ControllerClientBackedSystemSchemaInitializer extends AbstractSystemSchemaInitializer {
  private static final Logger LOGGER = LogManager.getLogger(ControllerClientBackedSystemSchemaInitializer.class);
  private final ControllerClient controllerClient;
  private final VeniceSystemStoreType systemStore;
  private final SchemaPresenceChecker schemaPresenceChecker;
  private final boolean autoCreateWriteSystemStores;

  public ControllerClientBackedSystemSchemaInitializer(
      ControllerClient controllerClient,
      VeniceSystemStoreType systemStore,
      SchemaPresenceChecker schemaPresenceChecker,
      boolean autoCreateWriteSystemStores) {
    this(controllerClient, systemStore, schemaPresenceChecker, autoCreateWriteSystemStores, null, false);
  }

  public ControllerClientBackedSystemSchemaInitializer(
      ControllerClient controllerClient,
      VeniceSystemStoreType systemStore,
      SchemaPresenceChecker schemaPresenceChecker,
      boolean autoCreateWriteSystemStores,
      UpdateStoreQueryParams updateStoreQueryParams,
      boolean autoRegisterDerivedComputeSchema) {
    super(systemStore, controllerClient.getClusterName(), updateStoreQueryParams, autoRegisterDerivedComputeSchema);
    this.controllerClient = controllerClient;
    this.systemStore = systemStore;
    this.schemaPresenceChecker = schemaPresenceChecker;
    this.autoCreateWriteSystemStores = autoCreateWriteSystemStores;
  }

  @Override
  protected String getActualSystemStoreCluster() {
    D2ServiceDiscoveryResponse response = controllerClient.discoverCluster(getSchemaStoreName());
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
    return autoCreateWriteSystemStores;
  }

  @Override
  protected void createSchemaStore(Schema firstKeySchema, Schema firstValueSchema) {
    NewStoreResponse newStoreResponse = controllerClient.createNewSystemStore(
        systemStore.getZkSharedStoreName(),
        VeniceConstants.SYSTEM_STORE_OWNER,
        firstKeySchema.toString(),
        firstValueSchema.toString());

    if (newStoreResponse.isError()) {
      throw new VeniceException(newStoreResponse.getError());
    }
  }

  @Override
  protected void updateStore(UpdateStoreQueryParams updateStoreQueryParams) {
    controllerClient.updateStore(getSchemaStoreName(), updateStoreQueryParams);
  }

  @Override
  protected SchemaEntry getRegisteredKeySchema() {
    SchemaResponse response = controllerClient.getKeySchema(getSchemaStoreName());
    return new SchemaEntry(response.getId(), AvroCompatibilityHelper.parse(response.getSchemaStr()));
  }

  @Override
  protected Collection<SchemaEntry> getAllRegisteredValueSchemas() {
    MultiSchemaResponse response = controllerClient.getAllValueSchema(getSchemaStoreName());

    return Arrays.stream(response.getSchemas())
        .map(schema -> new SchemaEntry(schema.getId(), AvroCompatibilityHelper.parse(schema.getSchemaStr())))
        .collect(Collectors.toSet());
  }

  @Override
  protected void registerValueSchema(Schema schema, int version) {
    controllerClient.addValueSchema(getSchemaStoreName(), schema.toString(), version);
  }

  @Override
  protected ValueAndDerivedSchemaId getDerivedSchemaInfo(Schema derivedSchema) {
    SchemaResponse response =
        controllerClient.getValueOrDerivedSchemaId(getSchemaStoreName(), derivedSchema.toString());
    return new ValueAndDerivedSchemaId(response.getId(), response.getDerivedSchemaId());
  }

  @Override
  protected void registerDerivedSchema(int valueSchemaVersion, Schema writeComputeSchema) {
    controllerClient.addDerivedSchema(getSchemaStoreName(), valueSchemaVersion, writeComputeSchema.toString());
  }

  @Override
  public void verify() {
    // Wait for newly registered schema to propagate to local fabric
    schemaPresenceChecker.verifySchemaVersionPresentOrExit();
  }
}
