package com.linkedin.venice.meta;

import com.linkedin.venice.exceptions.StoreVersionNotFoundException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.views.MaterializedView;
import com.linkedin.venice.views.VeniceView;
import com.linkedin.venice.views.ViewUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


public class ReadOnlyViewStore extends ReadOnlyStore {
  private final Map<Integer, ReadOnlyVersion> viewVersionMap;
  private final String viewStoreName;

  public ReadOnlyViewStore(Store delegate, String viewStoreName) {
    super(delegate);
    this.viewVersionMap = new HashMap<>();
    this.viewStoreName = viewStoreName;
    // Decorate the Store with appropriate version list based on the provided view
    List<Version> storeVersions = delegate.getVersions();
    String viewName = VeniceView.getViewNameFromViewStoreName(viewStoreName);
    for (Version version: storeVersions) {
      ViewConfig viewConfig = version.getViewConfigs().get(viewName);
      if (viewConfig == null) {
        // versions that do not contain the corresponding view name is omitted
        continue;
      }
      viewVersionMap
          .put(version.getNumber(), new ReadOnlyViewStore.ReadOnlyMaterializedViewVersion(version, viewConfig));
    }
  }

  @Override
  public String getName() {
    return viewStoreName;
  }

  @Override
  public List<Version> getVersions() {
    return new ArrayList<>(viewVersionMap.values());
  }

  @Override
  @Nullable
  public Version getVersion(int versionNumber) {
    return viewVersionMap.get(versionNumber);
  }

  @Override
  @Nonnull
  public Version getVersionOrThrow(int versionNumber) throws StoreVersionNotFoundException {
    Version version = viewVersionMap.get(versionNumber);
    if (version == null) {
      throw new StoreVersionNotFoundException(getName(), versionNumber);
    }
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReadOnlyViewStore storeView = (ReadOnlyViewStore) o;
    return this.delegate.equals(storeView.delegate) && this.viewVersionMap.equals(storeView.viewVersionMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(delegate, viewVersionMap);
  }

  /**
   * A read only {@link Version} representation of a materialized view for consumers (e.g. DVC client). Any view
   * specific version properties will be overwritten here and provided by the corresponding {@link MaterializedView}.
   */
  public static class ReadOnlyMaterializedViewVersion extends ReadOnlyStore.ReadOnlyVersion {
    private static final String CONSTRUCTOR_ERROR_MESSAGE = "Cannot construct materialized view version because ";
    private final MaterializedView materializedView;
    private final String materializedViewTopicName;

    public ReadOnlyMaterializedViewVersion(Version delegate, ViewConfig viewConfig) {
      super(delegate);
      if (viewConfig == null) {
        throw new VeniceException(CONSTRUCTOR_ERROR_MESSAGE + "provided viewConfig is null");
      }
      VeniceView veniceView = ViewUtils.getVeniceView(
          viewConfig.getViewClassName(),
          new Properties(),
          delegate.getStoreName(),
          viewConfig.getViewParameters());
      if (!(veniceView instanceof MaterializedView)) {
        throw new VeniceException(
            CONSTRUCTOR_ERROR_MESSAGE + viewConfig.getViewClassName() + " is not a "
                + MaterializedView.class.getCanonicalName());
      }
      this.materializedView = (MaterializedView) veniceView;
      Optional<String> optionalMaterializedViewTopicName =
          materializedView.getTopicNamesAndConfigsForVersion(delegate.getNumber()).keySet().stream().findAny();
      if (!optionalMaterializedViewTopicName.isPresent()) {
        throw new VeniceException(
            CONSTRUCTOR_ERROR_MESSAGE + "view topic map is empty for view: " + materializedView.getViewName()
                + ", version: " + delegate.getNumber());
      }
      this.materializedViewTopicName = optionalMaterializedViewTopicName.get();
    }

    @Override
    public int getPartitionCount() {
      return materializedView.getViewPartitionCount();
    }

    @Override
    public String kafkaTopicName() {
      return materializedViewTopicName;
    }

    @Override
    public PartitionerConfig getPartitionerConfig() {
      return materializedView.getPartitionerConfig();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReadOnlyMaterializedViewVersion version = (ReadOnlyMaterializedViewVersion) o;
      return this.delegate.equals(version.delegate) && this.materializedView.equals(version.materializedView);
    }

    @Override
    public int hashCode() {
      return Objects.hash(delegate, materializedView);
    }

    @Override
    public String getStoreName() {
      return VeniceView.getViewStoreName(delegate.getStoreName(), materializedView.getViewName());
    }
  }
}
