package com.linkedin.venice.common;

import com.linkedin.venice.authorization.AceEntry;
import com.linkedin.venice.authorization.AclBinding;
import com.linkedin.venice.authorization.Method;
import com.linkedin.venice.authorization.Permission;
import com.linkedin.venice.authorization.Resource;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Enum used to differentiate the different types of Venice system stores when access their metadata. Currently only
 * the store metadata system stores are treated differently because they are sharing metadata in Zookeeper. Future system
 * store types should be added here especially if they also would like to share metadata in Zookeeper.
 */
public enum VeniceSystemStoreType {
  DAVINCI_PUSH_STATUS_STORE(
      String.format(Store.SYSTEM_STORE_FORMAT, "davinci_push_status_store"), true, false,
      AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE_KEY,
      AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE,
      AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getSystemStoreName(), true, Method.WRITE_SYSTEM_STORE
  ),

  // New Metadata system store
  META_STORE(
      String.format(Store.SYSTEM_STORE_FORMAT, "meta_store"), true, false,
      AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE_KEY, AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE,
      AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getSystemStoreName(), true, Method.READ_SYSTEM_STORE
  ),

  // These system store's prefixes are used as their full name since they are not per-user-store system stores
  BATCH_JOB_HEARTBEAT_STORE(
      String.format(Store.SYSTEM_STORE_FORMAT, AvroProtocolDefinition.BATCH_JOB_HEARTBEAT), false, false,
      AvroProtocolDefinition.BATCH_JOB_HEARTBEAT_KEY, AvroProtocolDefinition.BATCH_JOB_HEARTBEAT, null, false,
      Method.WRITE_SYSTEM_STORE
  ),

  PARTITION_STATE(
      String.format(Store.SYSTEM_STORE_FORMAT, AvroProtocolDefinition.PARTITION_STATE), false, true,
      AvroProtocolDefinition.DEFAULT_SYSTEM_STORE_KEY_SCHEMA, AvroProtocolDefinition.PARTITION_STATE,
      AvroProtocolDefinition.PARTITION_STATE.getSystemStoreName(), false, Method.SCHEMA_SYSTEM_STORE
  ),

  STORE_VERSION_STATE(
      String.format(Store.SYSTEM_STORE_FORMAT, AvroProtocolDefinition.STORE_VERSION_STATE), false, true,
      AvroProtocolDefinition.DEFAULT_SYSTEM_STORE_KEY_SCHEMA, AvroProtocolDefinition.STORE_VERSION_STATE,
      AvroProtocolDefinition.STORE_VERSION_STATE.getSystemStoreName(), false, Method.SCHEMA_SYSTEM_STORE
  ),

  KAFKA_MESSAGE_ENVELOPE(
      String.format(Store.SYSTEM_STORE_FORMAT, AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE), false, true,
      AvroProtocolDefinition.DEFAULT_SYSTEM_STORE_KEY_SCHEMA, AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE,
      AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName(), false, Method.SCHEMA_SYSTEM_STORE
  ),

  TEST_DUMMY(
      String.format(Store.SYSTEM_STORE_FORMAT, AvroProtocolDefinition.TEST_DUMMY), false, true,
      AvroProtocolDefinition.DEFAULT_SYSTEM_STORE_KEY_SCHEMA, AvroProtocolDefinition.TEST_DUMMY,
      AvroProtocolDefinition.TEST_DUMMY.getSystemStoreName(), false, Method.SCHEMA_SYSTEM_STORE
  );

  private final String prefix;
  private final boolean isStoreZkShared;
  private final boolean isSchemaSystemStore;
  private AvroProtocolDefinition keySchemaProtocol;
  private AvroProtocolDefinition valueSchemaProtocol;
  private final String zkSharedStoreName;
  /**
   * Whether this specific type has adopted the new metadata repositories, such as
   * {@link com.linkedin.venice.helix.HelixReadOnlyZKSharedSystemStoreRepository}
   * {@link com.linkedin.venice.helix.HelixReadOnlyStoreRepositoryAdapter}
   * {@link com.linkedin.venice.helix.HelixReadWriteStoreRepositoryAdapter}
   *
   * When some specific system store type adopts the new repository arch, it needs to follow the following design pattern:
   * 1. Having the zk shared system store only created in system store cluster.
   * 2. This specific store type needs to onboard {@link com.linkedin.venice.meta.SystemStore} structure, which will
   *    extract the common store property from its zk shared system store and distinct properties from the corresponding
   *    regular venice store structure (such as version related metadata).
   */
  private final boolean newMetadataRepositoryAdopted;
  /**
   * The access {@link Method} type required by client (DaVinci or fast-client) for the system store.
   */
  private final Method clientAccessMethod;

  public static final List<VeniceSystemStoreType> VALUES = Collections.unmodifiableList(Arrays.asList(values()));

  VeniceSystemStoreType(
      String prefix,
      boolean isStoreZkShared,
      boolean isSchemaSystemStore,
      AvroProtocolDefinition keySchemaProtocol,
      AvroProtocolDefinition valueSchemaProtocol,
      String zkSharedStoreName,
      boolean newMetadataRepositoryAdopted,
      Method clientAccessMethod) {
    this.prefix = prefix;
    this.isStoreZkShared = isStoreZkShared;
    this.isSchemaSystemStore = isSchemaSystemStore;
    this.keySchemaProtocol = keySchemaProtocol;
    this.valueSchemaProtocol = valueSchemaProtocol;
    if (zkSharedStoreName == null) {
      this.zkSharedStoreName = this.getPrefix();
    } else {
      this.zkSharedStoreName = zkSharedStoreName;
    }
    this.newMetadataRepositoryAdopted = newMetadataRepositoryAdopted;
    this.clientAccessMethod = clientAccessMethod;
  }

  public String getPrefix() {
    return prefix;
  }

  public boolean isStoreZkShared() {
    return isStoreZkShared;
  }

  public boolean isSchemaSystemStore() {
    return isSchemaSystemStore;
  }

  public String getZkSharedStoreName() {
    return zkSharedStoreName;
  }

  public String getZkSharedStoreNameInCluster(String clusterName) {
    return isNewMetadataRepositoryAdopted()
        ? zkSharedStoreName
        : zkSharedStoreName + VeniceSystemStoreUtils.SEPARATOR + clusterName;
  }

  @Deprecated
  public boolean isNewMedataRepositoryAdopted() {
    return isNewMetadataRepositoryAdopted();
  }

  public boolean isNewMetadataRepositoryAdopted() {
    return newMetadataRepositoryAdopted;
  }

  /**
   * This function is to compose a system store name according to the current system store type and the regular store name.
   * @param regularStoreName
   * @return
   */
  public String getSystemStoreName(String regularStoreName) {
    if (isSchemaSystemStore) {
      return getZkSharedStoreName();
    }
    return getPrefix() + VeniceSystemStoreUtils.SEPARATOR + regularStoreName;
  }

  /**
   * This function is used to check whether the passed store name belongs to the current system store type.
   * @param storeName
   * @return
   */
  public boolean isSystemStore(String storeName) {
    return storeName.startsWith(getPrefix());
  }

  /**
   * This function is to extract the regular store name from the system store name.
   * @param systemStoreName
   * @return
   */
  public String extractRegularStoreName(String systemStoreName) {
    if (isSystemStore(systemStoreName)) {
      return systemStoreName.substring((getPrefix() + VeniceSystemStoreUtils.SEPARATOR).length());
    }
    throw new IllegalArgumentException(
        "Invalid system store name: " + systemStoreName + " for system store type: " + name());
  }

  public Method getClientAccessMethod() {
    return clientAccessMethod;
  }

  public AvroProtocolDefinition getKeySchemaProtocol() {
    return keySchemaProtocol;
  }

  public AvroProtocolDefinition getValueSchemaProtocol() {
    return valueSchemaProtocol;
  }

  /**
   * Generate the corresponding AclBinding for a given Venice system store type based on the corresponding Venice store's
   * Read acl. i.e. all principals that are allowed to read a Venice store is allowed to read or write to its corresponding
   * Venice system store topics.
   * @param regularStoreAclBinding of the associated Venice store.
   * @return AclBinding to get read or write access for the corresponding Venice system store.
   */
  public AclBinding generateSystemStoreAclBinding(AclBinding regularStoreAclBinding) {
    String regularStoreName = regularStoreAclBinding.getResource().getName();
    if (!Store.isValidStoreName(regularStoreName)) {
      throw new UnsupportedOperationException(
          "Cannot generate system store AclBinding for a non-store resource: " + regularStoreName);
    }
    if (VeniceSystemStoreType.getSystemStoreType(regularStoreName) != null) {
      throw new UnsupportedOperationException(
          "Cannot generate system store AclBinding for a Venice system store: " + regularStoreName);
    }
    Resource systemStoreResource = new Resource(getSystemStoreName(regularStoreName));
    AclBinding systemStoreAclBinding = new AclBinding(systemStoreResource);
    for (AceEntry aceEntry: regularStoreAclBinding.getAceEntries()) {
      if (aceEntry.getMethod() == Method.Read && aceEntry.getPermission() == Permission.ALLOW) {
        AceEntry systemStoreAceEntry =
            new AceEntry(aceEntry.getPrincipal(), getClientAccessMethod(), aceEntry.getPermission());
        systemStoreAclBinding.addAceEntry(systemStoreAceEntry);
      }
    }
    return systemStoreAclBinding;
  }

  /**
   * AclBinding of system store resource retrieved from authorizer service will not have the system store specific
   * Method types and instead will either be READ or WRITE. This method transform a kafka topic AclBinding to a system
   * store AclBinding.
   * @param systemStoreTopicAclBinding AclBinding for the system store's kafka topic.
   * @return
   */
  public static AclBinding getSystemStoreAclFromTopicAcl(AclBinding systemStoreTopicAclBinding) {
    AclBinding systemStoreAclBinding = new AclBinding(systemStoreTopicAclBinding.getResource());
    for (AceEntry aceEntry: systemStoreTopicAclBinding.getAceEntries()) {
      Method method = aceEntry.getMethod();
      if (method == Method.Read) {
        method = Method.READ_SYSTEM_STORE;
      } else if (method == Method.Write) {
        method = Method.WRITE_SYSTEM_STORE;
      }
      systemStoreAclBinding.addAceEntry(new AceEntry(aceEntry.getPrincipal(), method, aceEntry.getPermission()));
    }
    return systemStoreAclBinding;
  }

  public static VeniceSystemStoreType getSystemStoreType(String storeName) {
    if (storeName == null) {
      return null;
    }
    for (VeniceSystemStoreType systemStoreType: VALUES) {
      if (storeName.startsWith(systemStoreType.getPrefix())) {
        return systemStoreType;
      }
    }
    return null;
  }

  /**
   * Get a list of enabled Venice system store types based on the given regular Venice Store object.
   * @param regularStore object to generate a list of enabled system store types with.
   * @return a list of enabled VeniceSystemStoreType
   */
  public static List<VeniceSystemStoreType> getEnabledSystemStoreTypes(Store regularStore) {
    if (VeniceSystemStoreType.getSystemStoreType(regularStore.getName()) != null) {
      // No system stores should be enabled for a system store.
      return Collections.emptyList();
    }
    List<VeniceSystemStoreType> enabledSystemStoreTypes = new ArrayList<>();
    if (regularStore.isDaVinciPushStatusStoreEnabled()) {
      enabledSystemStoreTypes.add(VeniceSystemStoreType.DAVINCI_PUSH_STATUS_STORE);
    }
    if (regularStore.isStoreMetaSystemStoreEnabled()) {
      enabledSystemStoreTypes.add(VeniceSystemStoreType.META_STORE);
    }
    return enabledSystemStoreTypes;
  }
}
