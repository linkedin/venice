package com.linkedin.venice.controller.init;

import com.linkedin.venice.VeniceConstants;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controller.VeniceControllerMultiClusterConfig;
import com.linkedin.venice.controller.VeniceHelixAdmin;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.init.AbstractSystemSchemaInitializer;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.utils.Pair;
import java.util.Collection;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A {@link AbstractSystemSchemaInitializer} impl to perform the actions necessary to register schema system stores as
 * a Leader controller
 */
public class LeaderControllerSystemSchemaInitializer extends AbstractSystemSchemaInitializer {
  private static final Logger LOGGER = LogManager.getLogger(LeaderControllerSystemSchemaInitializer.class);

  private final VeniceSystemStoreType systemStore;
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final VeniceHelixAdmin admin;

  public LeaderControllerSystemSchemaInitializer(
      VeniceSystemStoreType systemStore,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      VeniceHelixAdmin admin) {
    this(systemStore, multiClusterConfigs, admin, null, false);
  }

  public LeaderControllerSystemSchemaInitializer(
      VeniceSystemStoreType systemStore,
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      VeniceHelixAdmin admin,
      UpdateStoreQueryParams storeMetadataUpdate,
      boolean autoRegisterDerivedComputeSchema) {
    super(
        systemStore,
        multiClusterConfigs.getSystemSchemaClusterName(),
        storeMetadataUpdate,
        autoRegisterDerivedComputeSchema);
    this.systemStore = systemStore;
    this.multiClusterConfigs = multiClusterConfigs;
    this.admin = admin;
  }

  @Override
  protected String getActualSystemStoreCluster() {
    try {
      Pair<String, String> clusterNameAndD2 = admin.discoverCluster(getSchemaStoreName());
      return clusterNameAndD2.getFirst();
    } catch (VeniceNoStoreException e) {
      return null;
    }
  }

  @Override
  protected boolean doesStoreExist() {
    return admin.getStore(getSchemaSystemStoreClusterName(), getSchemaStoreName()) != null;
  }

  @Override
  protected boolean shouldCreateMissingSchemaStores() {
    return true;
  }

  @Override
  protected void createSchemaStore(Schema firstKeySchema, Schema firstValueSchema) {
    admin.createStore(
        getSchemaSystemStoreClusterName(),
        getSchemaStoreName(),
        VeniceConstants.SYSTEM_STORE_OWNER,
        firstKeySchema.toString(),
        firstValueSchema.toString(),
        true);
  }

  @Override
  protected void updateStore(UpdateStoreQueryParams updateStoreQueryParams) {
    admin.updateStore(getSchemaSystemStoreClusterName(), getSchemaStoreName(), updateStoreQueryParams);
  }

  @Override
  protected SchemaEntry getRegisteredKeySchema() {
    return admin.getKeySchema(getSchemaSystemStoreClusterName(), getSchemaStoreName());
  }

  @Override
  protected Collection<SchemaEntry> getAllRegisteredValueSchemas() {
    return admin.getValueSchemas(getSchemaSystemStoreClusterName(), getSchemaStoreName());
  }

  @Override
  protected void registerValueSchema(Schema schema, int version) {
    admin.addValueSchema(
        getSchemaSystemStoreClusterName(),
        getSchemaStoreName(),
        schema.toString(),
        version,
        DirectionalSchemaCompatibilityType.NONE,
        false);
  }

  @Override
  protected ValueAndDerivedSchemaId getDerivedSchemaInfo(Schema writeComputeSchema) {
    Pair<Integer, Integer> valueAndDerivedSchemaIdPair = admin
        .getDerivedSchemaId(getSchemaSystemStoreClusterName(), getSchemaStoreName(), writeComputeSchema.toString());
    return new ValueAndDerivedSchemaId(valueAndDerivedSchemaIdPair.getFirst(), valueAndDerivedSchemaIdPair.getSecond());
  }

  @Override
  protected void registerDerivedSchema(int valueSchemaVersion, Schema writeComputeSchema) {
    admin.addDerivedSchema(
        getSchemaSystemStoreClusterName(),
        getSchemaStoreName(),
        valueSchemaVersion,
        writeComputeSchema.toString());
  }

  @Override
  public String toString() {
    return "LeaderControllerSystemSchemaInitializer{systemStore=" + systemStore + '}';
  }
}
