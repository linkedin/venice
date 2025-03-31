package com.linkedin.venice.common;

import com.linkedin.venice.annotation.VisibleForTesting;
import com.linkedin.venice.authorization.AceEntry;
import com.linkedin.venice.authorization.AclBinding;
import com.linkedin.venice.authorization.Method;
import com.linkedin.venice.authorization.Permission;
import com.linkedin.venice.authorization.Resource;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreName;
import com.linkedin.venice.pushstatus.PushStatusKey;
import com.linkedin.venice.pushstatus.PushStatusValue;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatKey;
import com.linkedin.venice.status.protocol.BatchJobHeartbeatValue;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * Enum used to differentiate the different types of Venice system stores when access their metadata. Currently only
 * the store metadata system stores are treated differently because they are sharing metadata in Zookeeper. Future system
 * store types should be added here especially if they also would like to share metadata in Zookeeper.
 *
 * @see <a href="https://venicedb.org/docs/ops_guide/system_stores">System Stores in the docs</a>
 */
public enum VeniceSystemStoreType {
  DAVINCI_PUSH_STATUS_STORE(
      VeniceSystemStoreUtils.DAVINCI_PUSH_STATUS_STORE_STR, true, PushStatusKey.SCHEMA$.toString(),
      PushStatusValue.SCHEMA$.toString(), AvroProtocolDefinition.PUSH_STATUS_SYSTEM_SCHEMA_STORE.getSystemStoreName(),
      true, Method.WRITE_SYSTEM_STORE
  ),

  // New Metadata system store
  META_STORE(
      VeniceSystemStoreUtils.META_STORE_STR, true, StoreMetaKey.SCHEMA$.toString(), StoreMetaValue.SCHEMA$.toString(),
      AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getSystemStoreName(), true, Method.READ_SYSTEM_STORE
  ),

  // This system store's prefix is used as its full name since it is not a per-user-store system store
  BATCH_JOB_HEARTBEAT_STORE(
      String.format(Store.SYSTEM_STORE_FORMAT, AvroProtocolDefinition.BATCH_JOB_HEARTBEAT), false,
      BatchJobHeartbeatKey.SCHEMA$.toString(), BatchJobHeartbeatValue.SCHEMA$.toString(), "", false,
      Method.WRITE_SYSTEM_STORE
  );

  private final String prefix;
  private final boolean isStoreZkShared;
  private final String keySchema;
  private final String valueSchema;
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
  private final boolean newMedataRepositoryAdopted;
  /**
   * The access {@link Method} type required by client (DaVinci or fast-client) for the system store.
   */
  private final Method clientAccessMethod;

  public static final List<VeniceSystemStoreType> VALUES = Collections.unmodifiableList(Arrays.asList(values()));

  VeniceSystemStoreType(
      String prefix,
      boolean isStoreZkShared,
      String keySchema,
      String valueSchema,
      String zkSharedStoreName,
      boolean newMedataRepositoryAdopted,
      Method clientAccessMethod) {
    this.prefix = prefix;
    this.isStoreZkShared = isStoreZkShared;
    this.keySchema = keySchema;
    this.valueSchema = valueSchema;
    if (zkSharedStoreName.isEmpty()) {
      this.zkSharedStoreName = this.getPrefix();
    } else {
      this.zkSharedStoreName = zkSharedStoreName;
    }
    this.newMedataRepositoryAdopted = newMedataRepositoryAdopted;
    this.clientAccessMethod = clientAccessMethod;
  }

  public String getPrefix() {
    return prefix;
  }

  public boolean isStoreZkShared() {
    return isStoreZkShared;
  }

  public String getKeySchema() {
    return keySchema;
  }

  public String getValueSchema() {
    return valueSchema;
  }

  public String getZkSharedStoreName() {
    return zkSharedStoreName;
  }

  public String getZkSharedStoreNameInCluster(String clusterName) {
    return isNewMedataRepositoryAdopted()
        ? zkSharedStoreName
        : zkSharedStoreName + VeniceSystemStoreUtils.SEPARATOR + clusterName;
  }

  public boolean isNewMedataRepositoryAdopted() {
    return newMedataRepositoryAdopted;
  }

  /**
   * This function is to compose a system store name according to the current system store type and the regular store name.
   * @param regularStoreName
   * @return
   */
  public String getSystemStoreName(String regularStoreName) {
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

  /**
   * Generate the corresponding AclBinding for a given Venice system store type based on the corresponding Venice store's
   * Read acl. i.e. all principals that are allowed to read a Venice store is allowed to read or write to its corresponding
   * Venice system store topics.
   * @param regularStoreAclBinding of the associated Venice store.
   * @return AclBinding to get read or write access for the corresponding Venice system store.
   */
  public AclBinding generateSystemStoreAclBinding(AclBinding regularStoreAclBinding) {
    String regularStoreName = regularStoreAclBinding.getResource().getName();
    if (!StoreName.isValidStoreName(regularStoreName)) {
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

  private static final Map<String, VeniceSystemStoreType> STORE_TYPE_CACHE = new VeniceConcurrentHashMap<>(64);

  /**
   * Retrieves the VeniceSystemStoreType for the given store name, using caching for improved performance.
   *
   * @param storeName The name of the store.
   * @return The corresponding VeniceSystemStoreType if found; otherwise, null.
   */
  public static VeniceSystemStoreType getSystemStoreType(String storeName) {
    if (storeName == null || storeName.isEmpty()) {
      return null;
    }
    // perform lookup before prefix check to avoid prefix check for cached entries
    VeniceSystemStoreType cachedStoreType = STORE_TYPE_CACHE.get(storeName);
    if (cachedStoreType != null || !storeName.startsWith(Store.SYSTEM_STORE_NAME_PREFIX)) {
      return cachedStoreType;
    }

    return STORE_TYPE_CACHE.computeIfAbsent(storeName, key -> {
      for (VeniceSystemStoreType systemStoreType: VALUES) {
        if (storeName.startsWith(systemStoreType.getPrefix()) && !systemStoreType.getPrefix().equals(storeName)) {
          return systemStoreType;
        }
      }
      return null;
    });
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

  /**
   * Extract the corresponding user store name from the given store name if it happens to be a system store.
   */
  public static String extractUserStoreName(String storeName) {
    String userStoreName = storeName;
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (systemStoreType != null) {
      userStoreName = systemStoreType.extractRegularStoreName(storeName);
    }
    return userStoreName;
  }

  @VisibleForTesting
  static Map<String, VeniceSystemStoreType> getStoreTypeCache() {
    return STORE_TYPE_CACHE;
  }
}
