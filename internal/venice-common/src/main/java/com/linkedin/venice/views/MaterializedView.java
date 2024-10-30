package com.linkedin.venice.views;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewParameterKeys;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public class MaterializedView extends VeniceView {
  public static final String MATERIALIZED_VIEW_TOPIC_SUFFIX = "_mv";
  private static final String MISSING_PARAMETER_MESSAGE = "%s is required for materialized view!";

  public MaterializedView(Properties props, Store store, Map<String, String> viewParameters) {
    super(props, store, viewParameters);
  }

  @Override
  public Map<String, VeniceProperties> getTopicNamesAndConfigsForVersion(int version) {
    VeniceProperties properties = new VeniceProperties(props);
    String viewName = viewParameters.get(ViewParameterKeys.MATERIALIZED_VIEW_NAME.name());
    return Collections.singletonMap(
        Version.composeKafkaTopic(store.getName(), version) + VIEW_TOPIC_SEPARATOR + viewName
            + MATERIALIZED_VIEW_TOPIC_SUFFIX,
        properties);
  }

  /**
   * {@link ViewParameterKeys#MATERIALIZED_VIEW_PARTITION_COUNT} is required to configure a new re-partition view.
   * {@link ViewParameterKeys#MATERIALIZED_VIEW_PARTITIONER} is optional. The re-partition view will use the store level
   * partitioner config if it's not specified in the view parameters.
   * {@link ViewParameterKeys#MATERIALIZED_VIEW_PARTITIONER_PARAMS} is optional.
   */
  @Override
  public void validateConfigs() {
    super.validateConfigs();
    String viewName = viewParameters.get(ViewParameterKeys.MATERIALIZED_VIEW_NAME.name());
    if (viewName == null) {
      throw new VeniceException(
          String.format(MISSING_PARAMETER_MESSAGE, ViewParameterKeys.MATERIALIZED_VIEW_NAME.name()));
    }
    if (store.getViewConfigs().containsKey(viewName)) {
      throw new VeniceException("A view config with the same view name already exist, view name: " + viewName);
    }
    String viewPartitioner = viewParameters.get(ViewParameterKeys.MATERIALIZED_VIEW_PARTITIONER.name());
    if (viewPartitioner == null) {
      throw new VeniceException(
          String.format(MISSING_PARAMETER_MESSAGE, ViewParameterKeys.MATERIALIZED_VIEW_PARTITIONER.name()));
    }
    try {
      Class.forName(viewPartitioner);
    } catch (ClassNotFoundException e) {
      throw new VeniceException("Cannot find partitioner class: " + viewPartitioner);
    }
    String partitionCountString = viewParameters.get(ViewParameterKeys.MATERIALIZED_VIEW_PARTITION_COUNT.name());
    if (partitionCountString == null) {
      throw new VeniceException(
          String.format(MISSING_PARAMETER_MESSAGE, ViewParameterKeys.MATERIALIZED_VIEW_PARTITION_COUNT.name()));
    }
    int viewPartitionCount = Integer.parseInt(partitionCountString);
    // A materialized view with the exact same partitioner and partition count as the store is not allwoed
    if (store.getPartitionCount() == viewPartitionCount
        && store.getPartitionerConfig().getPartitionerClass().equals(viewPartitioner)) {
      throw new VeniceException(
          "A materialized view with the same partitioner and partition count as the original store is not allowed!");
    }
    // Check if there is already a materialized view with identical configs
    for (Map.Entry<String, ViewConfig> viewConfigEntries: store.getViewConfigs().entrySet()) {
      ViewConfig viewConfig = viewConfigEntries.getValue();
      if (viewConfig.getViewClassName().equals(MaterializedView.class.getCanonicalName())) {
        String configPartitioner =
            viewConfig.getViewParameters().get(ViewParameterKeys.MATERIALIZED_VIEW_PARTITIONER.name());
        int configPartitionCount = Integer
            .parseInt(viewConfig.getViewParameters().get(ViewParameterKeys.MATERIALIZED_VIEW_PARTITION_COUNT.name()));
        if (configPartitionCount == viewPartitionCount && configPartitioner.equals(viewPartitioner)) {
          throw new VeniceException(
              "A view with identical view configs already exist, view name: " + viewConfigEntries.getKey());
        }
      }
    }
  }
}
