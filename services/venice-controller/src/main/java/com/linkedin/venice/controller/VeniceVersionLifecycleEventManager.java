package com.linkedin.venice.controller;

import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;


/**
 * Event manager for {@link VeniceVersionLifecycleEventListener}. To start and to keep it simple we are only passing
 * the read-only version to the listeners along with the isSourceCluster flag to indicate if the event is from the
 * source cluster or not. This is useful for certain listeners that only care about the source cluster events during a
 * store migration. If needed we can also pass along the corresponding {@link HelixVeniceClusterResources} to the
 * listeners for more complex use cases.
 */
public class VeniceVersionLifecycleEventManager {
  private final List<VeniceVersionLifecycleEventListener> listeners;

  public VeniceVersionLifecycleEventManager() {
    listeners = new ArrayList<>();
  }

  public static void onCurrentVersionChanged(
      VeniceVersionLifecycleEventManager manager,
      String clusterName,
      Store store,
      int newCurrentVersion,
      int previousCurrentVersion,
      boolean isFutureVersion,
      boolean isMigrating,
      BiFunction<String, String, Boolean> isSourceClusterFn) {
    boolean isSourceCluster = true;
    if (isMigrating) {
      isSourceCluster = isSourceClusterFn.apply(clusterName, store.getName());
    }
    if (newCurrentVersion != Store.NON_EXISTING_VERSION) {
      if (isFutureVersion) {
        manager.notifyVersionBecomingCurrentFromFuture(store.getVersionOrThrow(newCurrentVersion), isSourceCluster);
      } else {
        manager.notifyVersionBecomingCurrentFromBackup(store.getVersionOrThrow(newCurrentVersion), isSourceCluster);
      }
    }
    if (previousCurrentVersion != Store.NON_EXISTING_VERSION) {
      manager.notifyVersionBecomingBackup(store.getVersionOrThrow(previousCurrentVersion), isSourceCluster);
    }
  }

  public void addListener(VeniceVersionLifecycleEventListener listener) {
    listeners.add(listener);
  }

  void notifyVersionCreated(Version version, boolean isSourceCluster) {
    Version readOnlyVersion = new ReadOnlyStore.ReadOnlyVersion(version);
    for (VeniceVersionLifecycleEventListener listener: listeners) {
      listener.onVersionCreated(readOnlyVersion, isSourceCluster);
    }
  }

  void notifyVersionDeleted(Version version, boolean isSourceCluster) {
    Version readOnlyVersion = new ReadOnlyStore.ReadOnlyVersion(version);
    for (VeniceVersionLifecycleEventListener listener: listeners) {
      listener.onVersionDeleted(readOnlyVersion, isSourceCluster);
    }
  }

  void notifyVersionBecomingCurrentFromFuture(Version version, boolean isSourceCluster) {
    Version readOnlyVersion = new ReadOnlyStore.ReadOnlyVersion(version);
    for (VeniceVersionLifecycleEventListener listener: listeners) {
      listener.onVersionBecomingCurrentFromFuture(readOnlyVersion, isSourceCluster);
    }
  }

  void notifyVersionBecomingCurrentFromBackup(Version version, boolean isSourceCluster) {
    Version readOnlyVersion = new ReadOnlyStore.ReadOnlyVersion(version);
    for (VeniceVersionLifecycleEventListener listener: listeners) {
      listener.onVersionBecomingCurrentFromBackup(readOnlyVersion, isSourceCluster);
    }
  }

  void notifyVersionBecomingBackup(Version version, boolean isSourceCluster) {
    Version readOnlyVersion = new ReadOnlyStore.ReadOnlyVersion(version);
    for (VeniceVersionLifecycleEventListener listener: listeners) {
      listener.onVersionBecomingBackup(readOnlyVersion, isSourceCluster);
    }
  }
}
