package com.linkedin.venice.helix;

import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.exceptions.VeniceStoreAlreadyExistsException;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.system.store.MetaStoreWriter;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.locks.AutoCloseableLock;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.AccessOption;
import org.apache.helix.zookeeper.impl.client.ZkClient;


/**
 * Read write store repository which uses Helix as storage.
 * <p>
 * This repository does NOT listen the change of store from ZK. Because in Venice, this is the only once place to modify
 * stores.
 *
 * <p>Persistence layout (only when {@code perVersionZnodeWriteEnabled} is true):
 * <ul>
 *   <li>{@code /<cluster>/Stores/<name>} holds the {@link Store} JSON. Its embedded versions list is a FROZEN set
 *       inherited from stores that pre-date the per-version-znode layout. The list is never appended to; it can only
 *       shrink (when the caller removes a legacy version) or have its entries mutated in place (e.g. via
 *       {@code updateVersionStatus}, which mutates the shared Avro record).</li>
 *   <li>{@code /<cluster>/Stores/<name>/versions/<n>} holds each non-legacy {@link Version} as its own JSON znode.
 *       Every newly added version lands here; mutations to existing znode-versions are propagated by re-writing the
 *       znode on the next {@code updateStore} call.</li>
 * </ul>
 *
 * <p>When the flag is false, the entire {@link Store} (including its versions list) is persisted to the single store
 * znode — the layout every Venice cluster used before this work. The read path is unconditionally smart and supports
 * both layouts so readers can roll out ahead of writers.
 *
 * <p>Invariant (split layout only): a given version number lives in exactly one of the two layers. The write path
 * enforces this by upserting target versions to {@code /versions/<n>} iff their number is NOT in the prior embedded
 * list; the read path ({@link CachedReadOnlyStoreRepository#hydrateVersionsFromZk(Store)}) throws on overlap. There is
 * no migration step that moves a legacy embedded version into a per-version znode.
 */
public class HelixReadWriteStoreRepository extends CachedReadOnlyStoreRepository implements ReadWriteStoreRepository {
  private final Optional<MetaStoreWriter> metaStoreWriter;
  private final String clusterName;
  /**
   * Gates the split write path. When false, the repo persists each store as a single znode with the entire versions
   * list embedded in the JSON — the layout every Venice cluster used before the per-version-znode work. When true,
   * writes go through {@link #writeStoreAndSplitVersions}, which routes new versions to {@code /versions/<n>}. The read
   * path is unconditionally smart, so this flag only flips after every reader on the cluster runs a build that
   * understands the split layout.
   */
  private final boolean perVersionZnodeWriteEnabled;

  public HelixReadWriteStoreRepository(
      ZkClient zkClient,
      HelixAdapterSerializer compositeSerializer,
      String clusterName,
      Optional<MetaStoreWriter> metaStoreWriter,
      ClusterLockManager storeLock) {
    this(zkClient, compositeSerializer, clusterName, metaStoreWriter, storeLock, false);
  }

  public HelixReadWriteStoreRepository(
      ZkClient zkClient,
      HelixAdapterSerializer compositeSerializer,
      String clusterName,
      Optional<MetaStoreWriter> metaStoreWriter,
      ClusterLockManager storeLock,
      boolean perVersionZnodeWriteEnabled) {
    super(zkClient, clusterName, compositeSerializer, storeLock);
    this.clusterName = clusterName;
    this.metaStoreWriter = metaStoreWriter;
    this.perVersionZnodeWriteEnabled = perVersionZnodeWriteEnabled;
  }

  @Override
  public void addStore(Store store) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(store.getName())) {
      if (hasStore(store.getName())) {
        throw new VeniceStoreAlreadyExistsException(store.getName(), clusterName);
      }
      writeStoreToZk(store);
      putStore(store);
    }
  }

  @Override
  public void updateStore(Store store) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(store.getName())) {
      if (!hasStore(store.getName())) {
        throw new VeniceNoStoreException(store.getName(), clusterName);
      }
      writeStoreToZk(store);
      putStore(store);
      if (store.isStoreMetaSystemStoreEnabled() && metaStoreWriter.isPresent()) {
        /**
         * Write the update to the meta system store RT topic.
         */
        metaStoreWriter.get().writeStoreProperties(clusterName, store);
      }
    }
  }

  @Override
  public void deleteStore(String storeName) {
    try (AutoCloseableLock ignore = clusterLockManager.createStoreWriteLock(storeName)) {
      if (!hasStore(storeName)) {
        throw new VeniceNoStoreException(storeName, clusterName);
      }
      // Always tear down per-version znodes even when the flag is off — a prior run with the flag on may have left
      // them behind, and {@link HelixVersionAccessor#removeAllVersionsForStore} is a no-op when the container is
      // absent.
      versionAccessor.removeAllVersionsForStore(storeName);
      HelixUtils.remove(zkDataAccessor, getStoreZkPath(storeName));
      removeStore(storeName);
    }
  }

  private void writeStoreToZk(Store store) {
    if (perVersionZnodeWriteEnabled) {
      writeStoreAndSplitVersions(store);
    } else {
      HelixUtils.update(zkDataAccessor, getStoreZkPath(store.getName()), store);
    }
  }

  @Override
  public Store getStore(String storeName) {
    Store store = storeMap.get(storeName);
    if (store != null) {
      return store.cloneStore();
    }
    return null;
  }

  public Store getStoreOrThrow(String storeName) throws VeniceNoStoreException {
    Store store = getStore(storeName);
    if (store == null) {
      throw new VeniceNoStoreException(storeName, clusterName);
    }
    return store;
  }

  @Override
  public List<Store> getAllStores() {
    return storeMap.values().stream().map(s -> s.cloneStore()).collect(Collectors.toList());
  }

  @Override
  public Store refreshOneStore(String storeName) {
    return getStore(storeName);
  }

  /**
   * Persist {@code store} across the split layout. The frozen embedded set is whatever the current ZK store znode
   * carries (empty for stores created by this code path; non-empty for legacy stores). Target versions whose number is
   * NOT in that frozen set are upserted as per-version znodes — this covers both newly added versions and existing
   * znode-versions that the caller may have mutated in place (e.g. status transitions). Per-version znodes for numbers
   * absent from the target are removed. The store znode is rewritten with the legacy-embedded subset of the target,
   * which also drops any legacy versions the caller deleted.
   */
  private void writeStoreAndSplitVersions(Store store) {
    String storeName = store.getName();
    List<Version> targetVersions = store.getVersions();
    Set<Integer> targetVersionNumbers =
        targetVersions.stream().map(Version::getNumber).collect(Collectors.toCollection(HashSet::new));

    Store priorOnZk = zkDataAccessor.get(getStoreZkPath(storeName), null, AccessOption.PERSISTENT);
    Set<Integer> frozenEmbeddedNumbers = new HashSet<>();
    if (priorOnZk != null) {
      for (Version v: priorOnZk.getVersions()) {
        frozenEmbeddedNumbers.add(v.getNumber());
      }
    }
    Set<Integer> existingZnodeNumbers = versionAccessor.getVersionNumbersForStore(storeName)
        .stream()
        .map(Integer::parseInt)
        .collect(Collectors.toCollection(HashSet::new));

    List<Version> embeddedKeep = new ArrayList<>();
    for (Version version: targetVersions) {
      // ZKStore.getVersions() returns ReadOnlyVersion wrappers; both setVersions on the store znode payload and
      // VersionJSONSerializer require VersionImpl, so unwrap unconditionally.
      Version unwrapped = version.cloneVersion();
      if (frozenEmbeddedNumbers.contains(version.getNumber())) {
        embeddedKeep.add(unwrapped);
      } else {
        versionAccessor.putVersion(storeName, unwrapped);
      }
    }
    for (Integer existing: existingZnodeNumbers) {
      if (!targetVersionNumbers.contains(existing)) {
        versionAccessor.removeVersion(storeName, existing);
      }
    }

    Store storeForZkPayload = store.cloneStore();
    storeForZkPayload.setVersions(embeddedKeep);
    HelixUtils.update(zkDataAccessor, getStoreZkPath(storeName), storeForZkPayload);
  }
}
