package com.linkedin.davinci.repository;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.ReadOnlyStoreView;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.views.VeniceView;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;


/**
 * Adapter that provides read only interface to access store, schema and cluster info using the underlying
 * {@link NativeMetadataRepository} for both regular Venice stores and Venice view stores. Intended for client
 * libraries like DaVinci and CC clients which need to consume and materialize store views.
 */
public class NativeMetadataRepositoryViewAdapter
    implements SubscriptionBasedReadOnlyStoreRepository, ReadOnlySchemaRepository, ClusterInfoProvider {
  private final NativeMetadataRepository nativeMetadataRepository;

  // Map of store name to a set of subscribed store name and store view name(s). Used to track subscription, so we only
  // unsubscribe from the internal store once all corresponding regular store and view store(s) are unsubscribed.
  private final Map<String, Set<String>> subscribedStoreMap = new VeniceConcurrentHashMap<>();

  public NativeMetadataRepositoryViewAdapter(NativeMetadataRepository nativeMetadataRepository) {
    this.nativeMetadataRepository = nativeMetadataRepository;
  }

  @Override
  public void refresh() {
    nativeMetadataRepository.refresh();
  }

  @Override
  public void clear() {
    nativeMetadataRepository.refresh();
  }

  @Override
  public String getVeniceCluster(String storeName) {
    return nativeMetadataRepository.getVeniceCluster(VeniceView.getStoreName(storeName));
  }

  @Override
  public SchemaEntry getKeySchema(String storeName) {
    return nativeMetadataRepository.getKeySchema(VeniceView.getStoreName(storeName));
  }

  @Override
  public SchemaEntry getValueSchema(String storeName, int id) {
    return nativeMetadataRepository.getValueSchema(VeniceView.getStoreName(storeName), id);
  }

  @Override
  public boolean hasValueSchema(String storeName, int id) {
    return nativeMetadataRepository.hasValueSchema(VeniceView.getStoreName(storeName), id);
  }

  @Override
  public int getValueSchemaId(String storeName, String valueSchemaStr) {
    return nativeMetadataRepository.getValueSchemaId(VeniceView.getStoreName(storeName), valueSchemaStr);
  }

  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    return nativeMetadataRepository.getValueSchemas(VeniceView.getStoreName(storeName));
  }

  @Override
  public SchemaEntry getSupersetOrLatestValueSchema(String storeName) {
    return nativeMetadataRepository.getSupersetOrLatestValueSchema(VeniceView.getStoreName(storeName));
  }

  @Override
  public SchemaEntry getSupersetSchema(String storeName) {
    return nativeMetadataRepository.getSupersetSchema(VeniceView.getStoreName(storeName));
  }

  @Override
  public GeneratedSchemaID getDerivedSchemaId(String storeName, String derivedSchemaStr) {
    return nativeMetadataRepository.getDerivedSchemaId(VeniceView.getStoreName(storeName), derivedSchemaStr);
  }

  @Override
  public DerivedSchemaEntry getDerivedSchema(String storeName, int valueSchemaId, int writeComputeSchemaId) {
    return nativeMetadataRepository
        .getDerivedSchema(VeniceView.getStoreName(storeName), valueSchemaId, writeComputeSchemaId);
  }

  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String storeName) {
    return nativeMetadataRepository.getDerivedSchemas(VeniceView.getStoreName(storeName));
  }

  @Override
  public DerivedSchemaEntry getLatestDerivedSchema(String storeName, int valueSchemaId) {
    return nativeMetadataRepository.getLatestDerivedSchema(VeniceView.getStoreName(storeName), valueSchemaId);
  }

  @Override
  public RmdSchemaEntry getReplicationMetadataSchema(
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId) {
    return nativeMetadataRepository
        .getReplicationMetadataSchema(VeniceView.getStoreName(storeName), valueSchemaId, replicationMetadataVersionId);
  }

  @Override
  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String storeName) {
    return nativeMetadataRepository.getReplicationMetadataSchemas(VeniceView.getStoreName(storeName));
  }

  @Override
  public Store getStore(String storeName) {
    Store store = nativeMetadataRepository.getStore(VeniceView.getStoreName(storeName));
    if (store == null) {
      return null;
    }
    if (!VeniceView.isViewStore(storeName)) {
      return store;
    }
    // It's a view store, so we need to annotate the version(s) before returning
    List<Version> storeVersions = store.getVersions();
    String viewName = VeniceView.getViewNameFromViewStoreName(storeName);
    Map<Integer, ReadOnlyStore.ReadOnlyVersion> viewVersionMap = new HashMap<>();
    for (Version version: storeVersions) {
      ViewConfig viewConfig = version.getViewConfigs().get(viewName);
      if (viewConfig == null) {
        // versions that do not contain the corresponding view name is omitted
        continue;
      }
      viewVersionMap
          .put(version.getNumber(), new ReadOnlyStoreView.ReadOnlyMaterializedViewVersion(version, viewConfig));
    }
    return new ReadOnlyStoreView(store, viewVersionMap);
  }

  @Override
  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    Store store = getStore(storeName);
    if (store != null) {
      return store;
    }
    throw new VeniceNoStoreException(storeName);
  }

  @Override
  public boolean hasStore(String storeName) {
    return nativeMetadataRepository.hasStore(VeniceView.getStoreName(storeName));
  }

  @Override
  public Store refreshOneStore(String storeName) {
    return nativeMetadataRepository.refreshOneStore(VeniceView.getStoreName(storeName));
  }

  @Override
  public List<Store> getAllStores() {
    return nativeMetadataRepository.getAllStores();
  }

  @Override
  public long getTotalStoreReadQuota() {
    return nativeMetadataRepository.getTotalStoreReadQuota();
  }

  @Override
  public void registerStoreDataChangedListener(StoreDataChangedListener listener) {
    nativeMetadataRepository.registerStoreDataChangedListener(listener);
  }

  @Override
  public void unregisterStoreDataChangedListener(StoreDataChangedListener listener) {
    nativeMetadataRepository.unregisterStoreDataChangedListener(listener);
  }

  @Override
  public int getBatchGetLimit(String storeName) {
    return nativeMetadataRepository.getBatchGetLimit(VeniceView.getStoreName(storeName));
  }

  @Override
  public boolean isReadComputationEnabled(String storeName) {
    return nativeMetadataRepository.isReadComputationEnabled(VeniceView.getStoreName(storeName));
  }

  @Override
  public void subscribe(String storeName) throws InterruptedException {
    String internalStoreName = VeniceView.getStoreName(storeName);
    nativeMetadataRepository.subscribe(internalStoreName);
    getSubscribedStoreViews(internalStoreName).add(storeName);
  }

  @Override
  public void unsubscribe(String storeName) {
    String internalStoreName = VeniceView.getStoreName(storeName);
    Set<String> subscribedStoreViews = getSubscribedStoreViews(internalStoreName);
    subscribedStoreViews.remove(storeName);
    if (subscribedStoreViews.isEmpty()) {
      nativeMetadataRepository.unsubscribe(internalStoreName);
    }
  }

  private Set<String> getSubscribedStoreViews(String internalStoreName) {
    return subscribedStoreMap.computeIfAbsent(internalStoreName, k -> new ConcurrentSkipListSet<>());
  }
}
