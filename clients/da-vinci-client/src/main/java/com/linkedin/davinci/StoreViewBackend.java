package com.linkedin.davinci;

import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.views.VeniceView;


/**
 * A store view for a given store can be treated as an independent store from DVC client's POV. This class provides
 * the view specific functionalities to interact with the {@link DaVinciBackend} to properly configure a DVC client that
 * is subscribed to a store view. Extending {@link StoreBackend} for common logics necessary to maintain the life cycle
 * of a store on DVC client. e.g. subscribing to store repo, managing current version, future version and version swap.
 */
public class StoreViewBackend extends StoreBackend {
  private final String viewName;
  private final String storeViewName;

  StoreViewBackend(DaVinciBackend backend, String storeName, String viewName) {
    super(backend, storeName, viewName);
    this.viewName = viewName;
    this.storeViewName = VeniceView.getStoreAndViewName(storeName, viewName);
  }

  @Override
  public void setManaged(boolean isManaged) {
    throw new UnsupportedOperationException("Managed clients is not supported for Venice store view yet");
  }

  @Override
  protected Version getCurrentVersion() {
    Version storeVersion = super.getCurrentVersion();
    return getMaterializedViewVersion(storeVersion);
  }

  @Override
  protected Version getLatestNonFaultyVersion() {
    Version storeVersion = super.getLatestNonFaultyVersion();
    return getMaterializedViewVersion(storeVersion);
  }

  @Override
  protected String getStoreName() {
    return storeViewName;
  }

  /**
   * Construct a {@link ReadOnlyStore.ReadOnlyMaterializedViewVersion} using the given store version and the view
   * specified for this {@link StoreViewBackend}
   * @param storeVersion
   * @return the corresponding {@link ReadOnlyStore.ReadOnlyMaterializedViewVersion} or null if the provided
   * {@link Version} is null or if the specified view cannot be found in the provided store version.
   */
  private Version getMaterializedViewVersion(Version storeVersion) {
    if (storeVersion == null || !storeVersion.getViewConfigs().containsKey(viewName)) {
      return null;
    }
    return new ReadOnlyStore.ReadOnlyMaterializedViewVersion(storeVersion, storeVersion.getViewConfigs().get(viewName));
  }
}
