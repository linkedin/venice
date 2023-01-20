package com.linkedin.venice.helix;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.controllerapi.UpdateStoragePersonaQueryParams;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreDataChangedListener;
import com.linkedin.venice.persona.StoragePersona;
import com.linkedin.venice.persona.StoragePersonaAccessor;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.AutoCloseableSingleLock;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * Cached wrapper for the {@link StoragePersonaAccessor} class, which keeps a map of store names to personas that
 * contain them, as well as caches Zookeeper personas.
 * Class Invariants:
 * <ul>
 *   <li>Each method is thread-safe.</li>
 *   <li>The state of the repository (including the cache) will not be affected by object mutations until update
 *   methods from the repository are called.</li>
 * </ul>
 *
 */
public class StoragePersonaRepository {
  private final StoragePersonaAccessor storagePersonaAccessor;

  /** Map from persona names to personas. */
  private Map<String, StoragePersona> personaMap = new VeniceConcurrentHashMap<>();
  /** Map from store names to personas that contain them. */
  private Map<String, String> storeNamePersonaMap = new VeniceConcurrentHashMap<>();
  private final Lock personaLock = new ReentrantLock();
  private final ReadOnlyStoreRepository storeRepository;

  public StoragePersonaRepository(
      String clusterName,
      ReadOnlyStoreRepository repository,
      HelixAdapterSerializer compositeSerializer,
      ZkClient zkClient) {
    this.storagePersonaAccessor = new StoragePersonaAccessor(clusterName, compositeSerializer, zkClient);
    this.storeRepository = repository;
    storeRepository.registerStoreDataChangedListener(new StoragePersonaStoreDataListener(this));
    refresh();
  }

  /**
   * Returns a persona with the given name within the cluster that the repository is located.
   * In the case that no persona with this name exists, this method returns null.
   */
  public StoragePersona getPersona(String name) {
    StoragePersona persona = personaMap.get(name);
    if (persona == null) {
      return null;
    }
    return new StoragePersona(persona);
  }

  public List<StoragePersona> getAllPersonas() {
    return personaMap.values().stream().map(StoragePersona::new).collect(Collectors.toList());
  }

  public boolean hasPersona(String name) {
    return personaMap.containsKey(name);
  }

  /**
   * Clears the cache and re-loads the cache data from Zookeeper.
   */
  public final void refresh() {
    personaLock.lock();
    try {
      List<StoragePersona> personas = storagePersonaAccessor.getAllPersonasFromZk();
      personaMap = new VeniceConcurrentHashMap<>();
      storeNamePersonaMap = new VeniceConcurrentHashMap<>();
      for (StoragePersona persona: personas) {
        cachePersona(persona);
      }
    } finally {
      personaLock.unlock();
    }
  }

  /**
   * Creates a new persona with the given parameters.  This method will throw an error if:
   * <ul>
   *   <li>A persona with the given name already exists in the cluster.</li>
   *   <li>One of the stores in storesToEnforce does not exist in the cluster.</li>
   *   <li>The total quota of the stores exceeds the given persona budget.</li>
   * </ul>
   */
  public void addPersona(String name, long quotaNumber, Set<String> storesToEnforce, Set<String> owners) {
    addPersona(new StoragePersona(name, quotaNumber, storesToEnforce, owners));
  }

  private void addPersona(StoragePersona persona) {
    try (AutoCloseableLock ignore = AutoCloseableSingleLock.of(personaLock)) {
      storagePersonaAccessor.createPersona(persona);
      cachePersona(persona);
    }
  }

  /**
   * Updates a persona with the given parameters.  This method will throw an error if:
   * <ul>
   *   <li>There is no persona with this name in the cluster.</li>
   *   <li>One of the stores in storesToEnforce does not exist in the cluster.</li>
   *   <li>The total quota of the stores exceeds the given persona budget.</li>
   * </ul>
   * On failure, the given persona, if it exists, will remain unchanged.
   */
  public void updatePersona(String personaName, UpdateStoragePersonaQueryParams params) {
    try (AutoCloseableLock ignore = AutoCloseableSingleLock.of(personaLock)) {
      StoragePersona persona = getPersona(personaName);
      params.applyParams(persona);
      updatePersona(persona);
    }
  }

  private void updatePersona(StoragePersona persona) {
    try (AutoCloseableLock ignore = AutoCloseableSingleLock.of(personaLock)) {
      StoragePersona oldPersona = getPersona(persona.getName());
      if (oldPersona.equals(persona)) {
        return;
      }
      storagePersonaAccessor.updatePersona(persona);
      Set<String> oldStoreNames = oldPersona.getStoresToEnforce();
      personaMap.put(persona.getName(), new StoragePersona(persona));
      for (String storeName: oldStoreNames) {
        storeNamePersonaMap.remove(storeName);
      }
      for (String storeName: persona.getStoresToEnforce()) {
        storeNamePersonaMap.put(storeName, persona.getName());
      }
    }
  }

  /**
   * Deletes the persona with the given name.  If no persona is found, this method does nothing.
   */
  public void deletePersona(String personaName) {
    deletePersona(personaMap.get(personaName));
  }

  private void deletePersona(StoragePersona persona) {
    try (AutoCloseableLock ignore = AutoCloseableSingleLock.of(personaLock)) {
      if (persona == null) {
        return;
      }
      storagePersonaAccessor.deletePersona(persona);
      removePersonaFromCache(persona);
    }
  }

  private void deleteStores(List<String> storeNames) {
    try (AutoCloseableLock ignore = AutoCloseableSingleLock.of(personaLock)) {
      for (String store: storeNames) {
        String personaName = storeNamePersonaMap.get(store);
        if (personaName != null) {
          StoragePersona newPersona = getPersona(personaName);
          newPersona.getStoresToEnforce().remove(store);
          updatePersona(newPersona);
          storeNamePersonaMap.remove(store);
        }
      }
    }
  }

  public StoragePersona getPersonaContainingStore(String storeName) {
    String personaName = storeNamePersonaMap.get(storeName);
    if (personaName == null)
      return null;
    return getPersona(personaName);
  }

  private boolean isStoreSetValid(StoragePersona persona, Optional<Store> additionalStore) {
    Set<String> setToValidate = new HashSet<>();
    if (additionalStore.isPresent())
      setToValidate.add(additionalStore.get().getName());
    setToValidate.addAll(persona.getStoresToEnforce());
    return setToValidate.stream()
        .allMatch(
            s -> storeRepository.hasStore(s) && !VeniceSystemStoreUtils.isSystemStore(s)
                && (storeNamePersonaMap.get(s) == null || storeNamePersonaMap.get(s).equals(persona.getName())));
  }

  private boolean isQuotaValid(StoragePersona persona, Optional<Store> additionalStore) {
    long totalStorage;
    Set<Store> stores = new HashSet<>();
    additionalStore.ifPresent(stores::add);
    Set<String> storeNames = stores.stream().map(Store::getName).collect(Collectors.toSet());
    /** If the store is currently being updated, use the updated version of the store, not the
     * stale version from the store repository. */
    Set<Store> personaStores =
        getPersonaStores(persona).stream().filter(s -> !storeNames.contains(s.getName())).collect(Collectors.toSet());
    stores.addAll(personaStores);
    totalStorage = stores.stream().mapToLong(Store::getStorageQuotaInByte).sum();
    return totalStorage <= persona.getQuotaNumber();
  }

  private boolean isOwnersValid(StoragePersona persona) {
    return !persona.getOwners().isEmpty();
  }

  public void validatePersona(String personaName, long quota, Set<String> storesToEnforce, Set<String> owners) {
    validatePersona(new StoragePersona(personaName, quota, storesToEnforce, owners));
  }

  public void validatePersonaUpdate(String personaName, UpdateStoragePersonaQueryParams params) {
    StoragePersona persona = getPersona(personaName);
    if (persona == null) {
      throw new VeniceException("Update failed: persona with name " + personaName + " does not exist in this cluster");
    }
    params.applyParams(persona);
    validatePersona(persona);
  }

  public void validatePersona(StoragePersona persona) {
    validateAddUpdatedStore(persona, Optional.empty());
  }

  public void validateAddUpdatedStore(StoragePersona persona, Optional<Store> store) {
    if (!isStoreSetValid(persona, store)) {
      throw new VeniceException(
          "Invalid store(s) provided: either not all stores exist within the cluster, "
              + "one store is already managed by a persona, or one store is a system store");
    } else if (!isQuotaValid(persona, store)) {
      throw new VeniceException("Invalid persona quota: total store quota exceeds persona quota");
    } else if (!isOwnersValid(persona)) {
      throw new VeniceException("Invalid owner(s) provided");
    }
  }

  /**
   * This operation assumes that stores which are added to the persona are not contained within any store, but does
   * not do any validity checks.
   */
  public void addStoresToPersona(String personaName, List<String> storeNames) {
    try (AutoCloseableLock ignore = AutoCloseableSingleLock.of(personaLock)) {
      StoragePersona persona = getPersona(personaName);
      Set<String> stores = persona.getStoresToEnforce();
      stores.addAll(storeNames);
      updatePersona(persona.getName(), new UpdateStoragePersonaQueryParams().setStoresToEnforce(stores));
    }
  }

  public boolean personaContainsStore(String personaName, String storeName) {
    StoragePersona persona = getPersona(personaName);
    if (persona == null) {
      return false;
    }
    return persona.getStoresToEnforce().contains(storeName);
  }

  private synchronized Set<Store> getPersonaStores(StoragePersona persona) {
    Set<String> stores = persona.getStoresToEnforce();
    return stores.stream().map(storeRepository::getStore).collect(Collectors.toSet());
  }

  private void cachePersona(StoragePersona persona) {
    personaMap.put(persona.getName(), new StoragePersona(persona));
    for (String storeName: persona.getStoresToEnforce()) {
      storeNamePersonaMap.put(storeName, persona.getName());
    }
  }

  private void removePersonaFromCache(StoragePersona persona) {
    personaMap.remove(persona.getName());
    Set<String> storesOwned = persona.getStoresToEnforce();
    for (String store: storesOwned) {
      /**
       * If the persona did not exist, its stores may not be in the cache.
       */
      storeNamePersonaMap.remove(store);
    }
  }

  /**
   * Helper class which listens to changes from a {@link ReadOnlyStoreRepository} and updates the caches and ZK of the
   * StoragePersonaRepository accordingly.
   */
  public static class StoragePersonaStoreDataListener implements StoreDataChangedListener {
    StoragePersonaRepository repository;

    public StoragePersonaStoreDataListener(StoragePersonaRepository repository) {
      this.repository = repository;
    }

    /** Currently, specifying a persona that a store should be added to is not supported.
     * Thus, this method does nothing for now, but may be implemented in future versions.
     */
    @Override
    public void handleStoreCreated(Store store) {
    }

    /** This function removes the given store (if it exists) from any existing personas, both in the cache and in ZK,
     * and then removes the mapping from the store to the personas.
     */
    @Override
    public void handleStoreDeleted(String storeName) {
      repository.deleteStores(Arrays.asList(storeName));
    }

    /** Currently, changing the size of a store while it is part of a persona is undefined behavior.
     * Thus, this method does nothing for now, but may be implemented in future versions.
     */
    @Override
    public void handleStoreChanged(Store store) {

    }

  }

}
