package com.linkedin.davinci.repository;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ClusterInfoProvider;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyViewStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.views.VeniceView;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


/**
 * Adapter that provides read only interface to access store, schema and cluster info using the underlying
 * {@link NativeMetadataRepository} for both regular Venice stores and Venice view stores. Intended for client
 * libraries like DaVinci and CC clients which need to consume and materialize store views.
 */
public class NativeMetadataRepositoryViewAdapter implements SubscriptionBasedReadOnlyStoreRepository,
    ReadOnlySchemaRepository, ClusterInfoProvider, SubscribedViewStoreProvider {
  private final NativeMetadataRepository nativeMetadataRepository;

  // Map of store name to a set of subscribed store name and store view name(s). Used to track subscription, so we only
  // unsubscribe from the internal store once all corresponding regular store and view store(s) are unsubscribed.
  private final Map<String, Set<String>> subscribedStoreMap = new VeniceConcurrentHashMap<>();

  // A map to track registered StoreDataChangedListener and the corresponding StoreDataChangedListenerViewAdapter.
  private final Map<StoreDataChangedListener, StoreDataChangedListener> storeDataChangedAdapterMap =
      new VeniceConcurrentHashMap<>();

  public NativeMetadataRepositoryViewAdapter(NativeMetadataRepository nativeMetadataRepository) {
    this.nativeMetadataRepository = nativeMetadataRepository;
  }

  @Override
  public void refresh() {
    nativeMetadataRepository.refresh();
  }

  @Override
  public void clear() {
    nativeMetadataRepository.clear();
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
    // It's a view store, so we need to create a ReadOnlyViewStore
    return new ReadOnlyViewStore(store, storeName);
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

  /**
   * This method will only return all the subscribed Venice stores and not include the view stores for the following
   * reasons:
   *   1. Currently, there is no usage of get all view stores.
   *   2. The purpose of this adapter is to allow consumers to work with view stores without having to leak view store
   *   specific logic everywhere. If the repository included view stores in getAllStores() then callers will need to
   *   understand and differentiate view stores in order to avoid unexpected behaviors.
   */
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
    StoreDataChangedListener dataChangedListenerAdapter = new StoreDataChangedListenerViewAdapter(listener, this);
    storeDataChangedAdapterMap.computeIfAbsent(listener, (ignored) -> {
      nativeMetadataRepository.registerStoreDataChangedListener(dataChangedListenerAdapter);
      return dataChangedListenerAdapter;
    });
  }

  @Override
  public void unregisterStoreDataChangedListener(StoreDataChangedListener listener) {
    storeDataChangedAdapterMap.computeIfPresent(listener, (ignored, dataChangedListenerAdapter) -> {
      nativeMetadataRepository.unregisterStoreDataChangedListener(dataChangedListenerAdapter);
      return null;
    });
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
    subscribedStoreMap.compute(internalStoreName, (ignored, subscribedStores) -> {
      if (subscribedStores == null) {
        subscribedStores = new ConcurrentSkipListSet<>();
      }
      subscribedStores.add(storeName);
      return subscribedStores;
    });
  }

  @Override
  public void unsubscribe(String storeName) {
    String internalStoreName = VeniceView.getStoreName(storeName);
    subscribedStoreMap.computeIfPresent(internalStoreName, (ignored, subscribedStores) -> {
      subscribedStores.remove(storeName);
      if (subscribedStores.isEmpty()) {
        nativeMetadataRepository.unsubscribe(internalStoreName);
        return null;
      }
      return subscribedStores;
    });
  }

  @Override
  public @Nonnull Set<String> getSubscribedViewStores(String storeName) {
    Set<String> subscribedStores = subscribedStoreMap.get(storeName);
    if (subscribedStores == null) {
      return Collections.emptySet();
    }
    return subscribedStores.stream().filter(VeniceView::isViewStore).collect(Collectors.toSet());
  }
}
