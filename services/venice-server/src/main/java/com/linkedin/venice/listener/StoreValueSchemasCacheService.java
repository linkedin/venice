package com.linkedin.venice.listener;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.helix.HelixReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class implements the fast value schema/latest value schema lookup with acceptable delay.
 * The reason to introduce this class is that we found two issues to use {@link HelixReadOnlySchemaRepository} directly
 * in read compute path:
 * 1. When ZK disconnect/re-connect happens, {@link HelixReadOnlySchemaRepository} will refresh
 * its local cache, which would cause an increased GC count in read compute since {@link HelixReadOnlySchemaRepository#refresh}
 * is holding a giant write lock and all the value schema/latest value schema lookups in the read compute requests will be blocked.
 * The GC count increase is significant (more than doubled in test cluster), which has been causing much higher CPU usage and higher latency;
 * 2. The schema objects returned by {@link HelixReadOnlySchemaRepository} for the same schema
 * are not always the same object since {@link HelixReadOnlySchemaRepository#refresh()} would always re-create new Schema objects,
 * which will cause the inefficient de-serializer lookup in {@link SerializerDeserializerFactory}, which will compare the schema objects
 * to find out the corresponding serializer/de-serializer (for read compute case, de-serializer is the concern). If the schema objects
 * are not the same, {@link Schema#hashCode()} and {@link Schema#equals(Object)} will be used, in Avro-1.7 or above, {@link Schema#hashCode()} is
 * optimized to only calculate once if it is read-only, but {@link Schema#equals(Object)} couldn't be avoided.
 *
 * Here how it works in this class:
 * 1. It maintains a mapping between stores and their value schemas and latest value schema;
 * 2. It will try to reuse the same {@link Schema} object for the same Schema Id within a store since value schema is immutable;
 * 3. It maintains a refresh thread to update the local cache periodically;
 *
 * In theory, all the schema lookups shouldn't be blocked by invoking the underlying {@link HelixReadOnlySchemaRepository} since in reality,
 * it will take a fair long time to register a new value schema/latest value schema and start using it in prod, so the periodical schema
 * refresh should be able to take care of the new value schema/latest value schema discovery.
 * Since the refresh is async, there is a delay there (at most 1 min), and it should be acceptable because of the previous assumption.
 *
 * So far, this class only supports value schema by id lookup and latest value schema lookup.
 */
public class StoreValueSchemasCacheService extends AbstractVeniceService implements ReadOnlySchemaRepository {
  private static final Logger LOGGER = LogManager.getLogger(StoreValueSchemasCacheService.class);

  private static class StoreValueSchemas {
    private final Map<Integer, SchemaEntry> valueSchemaMap = new VeniceConcurrentHashMap<>();
    private SchemaEntry latestValueSchema;
  }

  private final ReadOnlyStoreRepository storeRepository;
  private final ReadOnlySchemaRepository schemaRepository;
  private final Map<String, StoreValueSchemas> storeValueSchemasMap = new VeniceConcurrentHashMap<>();

  private final Thread refreshThread;
  private boolean refreshThreadStop = false;

  public StoreValueSchemasCacheService(
      ReadOnlyStoreRepository storeRepository,
      ReadOnlySchemaRepository schemaRepository) {
    this.storeRepository = storeRepository;
    this.schemaRepository = schemaRepository;
    refreshAllStores();

    this.refreshThread = new Thread(new CacheRefreshTask(), "StoreValueSchemasCacheService_RefreshThread");
  }

  private class CacheRefreshTask implements Runnable {
    @Override
    public void run() {
      while (!refreshThreadStop) {
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(60));
          refreshAllStores();
        } catch (InterruptedException e) {
          break;
        }
      }
    }
  }

  @Override
  public boolean startInner() throws Exception {
    this.refreshThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    this.refreshThread.interrupt();
    refreshThreadStop = true;
  }

  @Override
  public SchemaEntry getValueSchema(String storeName, int valueSchemaId) {
    StoreValueSchemas storeValueSchemas =
        storeValueSchemasMap.computeIfAbsent(storeName, s -> refreshStoreValueSchemas(storeName));
    SchemaEntry valueSchemaEntry = storeValueSchemas.valueSchemaMap.get(valueSchemaId);
    if (valueSchemaEntry == null) {
      /**
       * Normally, this shouldn't happen since the refresh thread should take care of the new value schema before it gets used,
       * but it may not be this case in unit tests and integration tests.
       *
       * Force refresh the requested store.
       */
      synchronized (this) {
        storeValueSchemas = refreshStoreValueSchemas(storeName);
        storeValueSchemasMap.put(storeName, storeValueSchemas);
        valueSchemaEntry = storeValueSchemas.valueSchemaMap.get(valueSchemaId);
        if (valueSchemaEntry == null) {
          throw new VeniceException("Unknown value schema id: " + valueSchemaId + " in store: " + storeName);
        }
      }
    }
    return valueSchemaEntry;
  }

  @Override
  public SchemaEntry getSupersetOrLatestValueSchema(String storeName) {
    StoreValueSchemas storeValueSchemas =
        storeValueSchemasMap.computeIfAbsent(storeName, s -> refreshStoreValueSchemas(storeName));
    return storeValueSchemas.latestValueSchema;
  }

  @Override
  public SchemaEntry getSupersetSchema(String storeName) {
    return schemaRepository.getSupersetSchema(storeName);
  }

  private StoreValueSchemas refreshStoreValueSchemas(String storeName) {
    if (!storeRepository.hasStore(storeName)) {
      throw new VeniceNoStoreException(storeName);
    }
    // fetch the latest value schema and all the value schemas
    SchemaEntry latestValueSchema = schemaRepository.getSupersetOrLatestValueSchema(storeName);
    Collection<SchemaEntry> valueSchemas = schemaRepository.getValueSchemas(storeName);
    StoreValueSchemas storeValueSchemas = storeValueSchemasMap.get(storeName);
    if (storeValueSchemas == null) {
      storeValueSchemas = new StoreValueSchemas();
    }
    for (SchemaEntry schemaEntry: valueSchemas) {
      /**
       * Try to use the same Schema object all the time, and the assumption here is that the mapping between
       * schema id and schema content never changes.
       */
      if (!storeValueSchemas.valueSchemaMap.containsKey(schemaEntry.getId())) {
        storeValueSchemas.valueSchemaMap.put(schemaEntry.getId(), schemaEntry);
      }
    }
    SchemaEntry latestValueSchemaEntryInValueSchemaMap =
        storeValueSchemas.valueSchemaMap.get(latestValueSchema.getId());
    if (latestValueSchemaEntryInValueSchemaMap == null) {
      LOGGER.warn(
          "For store: {}, the latest value schema: {} is not part of all the value schemas: {}",
          storeName,
          latestValueSchema.getId(),
          storeValueSchemas.valueSchemaMap.keySet());
      storeValueSchemas.latestValueSchema = latestValueSchema;
    } else {
      /**
       * Try to use the same Schema object.
       */
      storeValueSchemas.latestValueSchema = storeValueSchemas.valueSchemaMap.get(latestValueSchema.getId());
    }

    return storeValueSchemas;
  }

  @Override
  public SchemaEntry getKeySchema(String storeName) {
    throw new VeniceException("Function: getKeySchema is not supported!");
  }

  @Override
  public boolean hasValueSchema(String storeName, int id) {
    throw new VeniceException("Function: getKeySchema is not supported!");
  }

  @Override
  public int getValueSchemaId(String storeName, String valueSchemaStr) {
    throw new VeniceException("Function: getValueSchemaId is not supported!");
  }

  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    throw new VeniceException("Function: getValueSchemas is not supported!");
  }

  @Override
  public GeneratedSchemaID getDerivedSchemaId(String storeName, String derivedSchemaStr) {
    throw new VeniceException("Function: getDerivedSchemaId is not supported!");
  }

  @Override
  public DerivedSchemaEntry getDerivedSchema(String storeName, int valueSchemaId, int derivedSchemaId) {
    throw new VeniceException("Function: getDerivedSchema is not supported!");
  }

  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String storeName) {
    throw new VeniceException("Function: getDerivedSchemas is not supported!");
  }

  @Override
  public DerivedSchemaEntry getLatestDerivedSchema(String storeName, int valueSchemaId) {
    throw new VeniceException("Function: getLatestDerivedSchema is not supported!");
  }

  @Override
  public RmdSchemaEntry getReplicationMetadataSchema(
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId) {
    throw new VeniceException("Function: getReplicationMetadataSchema is not supported!");
  }

  @Override
  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String storeName) {
    throw new VeniceException("Function: getReplicationMetadataSchemas is not supported!");
  }

  private void refreshAllStores() {
    List<Store> storeList = storeRepository.getAllStores();
    for (Store store: storeList) {
      try {
        refreshStoreValueSchemas(store.getName());
      } catch (Exception e) {
        LOGGER.error("Got exception while refreshing value schemas for store: {}", store.getName());
      }
    }
  }

  @Override
  public void refresh() {
    // Do nothing
  }

  @Override
  public void clear() {
    // Do nothing
  }
}
