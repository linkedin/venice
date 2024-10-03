package com.linkedin.venice.views;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewParameterKeys;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public class RePartitionView extends VeniceView {
  public static final String RE_PARTITION_TOPIC_SUFFIX = "_rp";
  private static final String MISSING_PARAMETER_MESSAGE = "%s is required for re-partition view!";

  public RePartitionView(Properties props, Store store, Map<String, String> viewParameters) {
    super(props, store, viewParameters);
  }

  @Override
  public Map<String, VeniceProperties> getTopicNamesAndConfigsForVersion(int version) {
    VeniceProperties properties = new VeniceProperties(props);
    String viewName = viewParameters.get(ViewParameterKeys.RE_PARTITION_VIEW_NAME.name());
    return Collections.singletonMap(
        Version.composeKafkaTopic(store.getName(), version) + VIEW_TOPIC_SEPARATOR + viewName
            + RE_PARTITION_TOPIC_SUFFIX,
        properties);
  }

  /**
   * {@link ViewParameterKeys#RE_PARTITION_VIEW_PARTITION_COUNT} is required to configure a new re-partition view.
   * {@link ViewParameterKeys#RE_PARTITION_VIEW_PARTITIONER} is optional. The re-partition view will use the store level
   * partitioner config if it's not specified in the view parameters.
   * {@link ViewParameterKeys#RE_PARTITION_VIEW_PARTITIONER_PARAMS} is optional.
   */
  @Override
  public void validateConfigs() {
    super.validateConfigs();
    String viewName = viewParameters.get(ViewParameterKeys.RE_PARTITION_VIEW_NAME.name());
    if (viewName == null) {
      throw new VeniceException(
          String.format(MISSING_PARAMETER_MESSAGE, ViewParameterKeys.RE_PARTITION_VIEW_NAME.name()));
    }
    String partitionCountString = viewParameters.get(ViewParameterKeys.RE_PARTITION_VIEW_PARTITION_COUNT.name());
    if (partitionCountString == null) {
      throw new VeniceException(
          String.format(MISSING_PARAMETER_MESSAGE, ViewParameterKeys.RE_PARTITION_VIEW_PARTITION_COUNT.name()));
    }
    // A re-partition view with the exact same partitioner and partition count makes no sense
    String partitioner = viewParameters.get(ViewParameterKeys.RE_PARTITION_VIEW_PARTITIONER.name());
    boolean samePartitioner =
        partitioner == null || partitioner.equals(store.getPartitionerConfig().getPartitionerClass());
    if (samePartitioner && store.getPartitionCount() == Integer.parseInt(partitionCountString)) {
      throw new VeniceException(
          "A re-partition view with the same partitioner and partition count as the original store is not allowed!");
    }
  }
}
