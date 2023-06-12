package com.linkedin.venice.helix;

import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadWriteSchemaRepository;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.avro.DirectionalSchemaCompatibilityType;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import java.util.Collection;


/**
 * This repository supports the following operations:
 * 1. For the regular stores, read/write operations are supported as {@link HelixReadWriteSchemaRepository}.
 * 2. For system stores, only read operations are supported via {@link HelixReadOnlyZKSharedSchemaRepository},
 *    and if you want to update the schemas for system stores, you will need to update the corresponding
 *    zk shared store instead.
 */
public class HelixReadWriteSchemaRepositoryAdapter implements ReadWriteSchemaRepository {
  private final HelixReadOnlyZKSharedSchemaRepository readOnlyZKSharedSchemaRepository;
  private final ReadWriteSchemaRepository readWriteRegularStoreSchemaRepository;

  public HelixReadWriteSchemaRepositoryAdapter(
      HelixReadOnlyZKSharedSchemaRepository readOnlyZKSharedSchemaRepository,
      ReadWriteSchemaRepository readWriteRegularStoreSchemaRepository) {
    this.readOnlyZKSharedSchemaRepository = readOnlyZKSharedSchemaRepository;
    this.readWriteRegularStoreSchemaRepository = readWriteRegularStoreSchemaRepository;
  }

  private String errorMsgForUnsupportedOperationsAgainstSystemStore(
      String storeName,
      VeniceSystemStoreType systemStoreType,
      String method) {
    return new StringBuilder("Method: '").append(method)
        .append("' can't be applied to store: ")
        .append(storeName)
        .append(" with system store type: ")
        .append(systemStoreType)
        .append(" directly, please update the corresponding zk shared store: ")
        .append(systemStoreType.getZkSharedStoreName())
        .append(" instead if necessary")
        .toString();
  }

  @Override
  public SchemaEntry initKeySchema(String storeName, String schemaStr) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.initKeySchema(storeName, schemaStr);
    }
    throw new VeniceException(
        errorMsgForUnsupportedOperationsAgainstSystemStore(storeName, systemStoreType, "initKeySchema"));
  }

  @Override
  public SchemaEntry addValueSchema(
      String storeName,
      String schemaStr,
      DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.addValueSchema(storeName, schemaStr, expectedCompatibilityType);
    }
    throw new VeniceException(
        errorMsgForUnsupportedOperationsAgainstSystemStore(storeName, systemStoreType, "addValueSchema"));
  }

  @Override
  public SchemaEntry addValueSchema(String storeName, String schemaStr, int schemaId) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.addValueSchema(storeName, schemaStr, schemaId);
    }
    throw new VeniceException(
        errorMsgForUnsupportedOperationsAgainstSystemStore(storeName, systemStoreType, "addValueSchema"));
  }

  @Override
  public DerivedSchemaEntry addDerivedSchema(String storeName, String schemaStr, int valueSchemaId) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.addDerivedSchema(storeName, schemaStr, valueSchemaId);
    }
    throw new VeniceException(
        errorMsgForUnsupportedOperationsAgainstSystemStore(storeName, systemStoreType, "addDerivedSchema"));
  }

  @Override
  public DerivedSchemaEntry addDerivedSchema(
      String storeName,
      String schemaStr,
      int valueSchemaId,
      int derivedSchemaId) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository
          .addDerivedSchema(storeName, schemaStr, valueSchemaId, derivedSchemaId);
    }
    throw new VeniceException(
        errorMsgForUnsupportedOperationsAgainstSystemStore(storeName, systemStoreType, "addDerivedSchema"));
  }

  @Override
  public DerivedSchemaEntry removeDerivedSchema(String storeName, int valueSchemaId, int derivedSchemaId) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.removeDerivedSchema(storeName, valueSchemaId, derivedSchemaId);
    }
    throw new VeniceException(
        errorMsgForUnsupportedOperationsAgainstSystemStore(storeName, systemStoreType, "removeDerivedSchema"));
  }

  @Override
  public int preCheckValueSchemaAndGetNextAvailableId(
      String storeName,
      String valueSchemaStr,
      DirectionalSchemaCompatibilityType expectedCompatibilityType) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository
          .preCheckValueSchemaAndGetNextAvailableId(storeName, valueSchemaStr, expectedCompatibilityType);
    }
    throw new VeniceException(
        errorMsgForUnsupportedOperationsAgainstSystemStore(
            storeName,
            systemStoreType,
            "preCheckValueSchemaAndGetNextAvailableId"));
  }

  @Override
  public int preCheckDerivedSchemaAndGetNextAvailableId(String storeName, int valueSchemaId, String derivedSchemaStr) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository
          .preCheckDerivedSchemaAndGetNextAvailableId(storeName, valueSchemaId, derivedSchemaStr);
    }
    throw new VeniceException(
        errorMsgForUnsupportedOperationsAgainstSystemStore(
            storeName,
            systemStoreType,
            "preCheckDerivedSchemaAndGetNextAvailableId"));
  }

  @Override
  public SchemaEntry getKeySchema(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.getKeySchema(storeName);
    }
    return readOnlyZKSharedSchemaRepository.getKeySchema(systemStoreType.getZkSharedStoreName());
  }

  @Override
  public SchemaEntry getValueSchema(String storeName, int id) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.getValueSchema(storeName, id);
    }
    return readOnlyZKSharedSchemaRepository.getValueSchema(systemStoreType.getZkSharedStoreName(), id);
  }

  @Override
  public boolean hasValueSchema(String storeName, int id) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.hasValueSchema(storeName, id);
    }
    return readOnlyZKSharedSchemaRepository.hasValueSchema(systemStoreType.getZkSharedStoreName(), id);
  }

  @Override
  public int getValueSchemaId(String storeName, String valueSchemaStr) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.getValueSchemaId(storeName, valueSchemaStr);
    }
    return readOnlyZKSharedSchemaRepository.getValueSchemaId(systemStoreType.getZkSharedStoreName(), valueSchemaStr);
  }

  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.getValueSchemas(storeName);
    }
    return readOnlyZKSharedSchemaRepository.getValueSchemas(systemStoreType.getZkSharedStoreName());
  }

  @Override
  public SchemaEntry getSupersetOrLatestValueSchema(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.getSupersetOrLatestValueSchema(storeName);
    }
    return readOnlyZKSharedSchemaRepository.getSupersetOrLatestValueSchema(systemStoreType.getZkSharedStoreName());
  }

  @Override
  public SchemaEntry getSupersetSchema(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.getSupersetSchema(storeName);
    }
    return readOnlyZKSharedSchemaRepository.getSupersetSchema(systemStoreType.getZkSharedStoreName());
  }

  @Override
  public GeneratedSchemaID getDerivedSchemaId(String storeName, String derivedSchemaStr) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.getDerivedSchemaId(storeName, derivedSchemaStr);
    }
    return readOnlyZKSharedSchemaRepository
        .getDerivedSchemaId(systemStoreType.getZkSharedStoreName(), derivedSchemaStr);
  }

  @Override
  public DerivedSchemaEntry getDerivedSchema(String storeName, int valueSchemaId, int derivedSchemaId) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.getDerivedSchema(storeName, valueSchemaId, derivedSchemaId);
    }
    return readOnlyZKSharedSchemaRepository
        .getDerivedSchema(systemStoreType.getZkSharedStoreName(), valueSchemaId, derivedSchemaId);
  }

  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.getDerivedSchemas(storeName);
    }
    return readOnlyZKSharedSchemaRepository.getDerivedSchemas(systemStoreType.getZkSharedStoreName());
  }

  @Override
  public DerivedSchemaEntry getLatestDerivedSchema(String storeName, int valueSchemaId) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.getLatestDerivedSchema(storeName, valueSchemaId);
    }
    return readOnlyZKSharedSchemaRepository
        .getLatestDerivedSchema(systemStoreType.getZkSharedStoreName(), valueSchemaId);
  }

  @Override
  public RmdSchemaEntry addReplicationMetadataSchema(
      String storeName,
      int valueSchemaId,
      String replicationMetadataSchemaStr,
      int replicationMetadataVersionId) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.addReplicationMetadataSchema(
          storeName,
          valueSchemaId,
          replicationMetadataSchemaStr,
          replicationMetadataVersionId);
    }
    throw new VeniceException(
        errorMsgForUnsupportedOperationsAgainstSystemStore(storeName, systemStoreType, "addMetadataSchema"));
  }

  @Override
  public RmdSchemaEntry getReplicationMetadataSchema(
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository
          .getReplicationMetadataSchema(storeName, valueSchemaId, replicationMetadataVersionId);
    }
    return readOnlyZKSharedSchemaRepository.getReplicationMetadataSchema(
        systemStoreType.getZkSharedStoreName(),
        valueSchemaId,
        replicationMetadataVersionId);
  }

  @Override
  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String storeName) {
    VeniceSystemStoreType systemStoreType = VeniceSystemStoreType.getSystemStoreType(storeName);
    if (HelixReadOnlyStoreRepositoryAdapter.forwardToRegularRepository(systemStoreType)) {
      return readWriteRegularStoreSchemaRepository.getReplicationMetadataSchemas(storeName);
    }
    return readOnlyZKSharedSchemaRepository.getReplicationMetadataSchemas(systemStoreType.getZkSharedStoreName());
  }

  @Override
  public void refresh() {
    readOnlyZKSharedSchemaRepository.refresh();
    readWriteRegularStoreSchemaRepository.refresh();
  }

  @Override
  public void clear() {
    readOnlyZKSharedSchemaRepository.clear();
    readWriteRegularStoreSchemaRepository.clear();
  }
}
