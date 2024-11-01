package com.linkedin.venice.views;

import static com.linkedin.venice.views.ViewUtils.PARTITION_COUNT;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.ViewConfig;
import com.linkedin.venice.meta.ViewParameters;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.utils.lazy.Lazy;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public class MaterializedView extends VeniceView {
  public static final String MATERIALIZED_VIEW_TOPIC_SUFFIX = "_mv";
  public static final String MATERIALIZED_VIEW_WRITER_CLASS_NAME =
      "com.linkedin.davinci.store.view.MaterializedViewWriter";
  private static final String MISSING_PARAMETER_MESSAGE = "%s is required for materialized view!";
  private final int viewPartitionCount;

  private Lazy<VenicePartitioner> viewPartitioner;

  public MaterializedView(Properties props, Store store, Map<String, String> viewParameters) {
    super(props, store, viewParameters);
    // Override topic partition count config
    viewPartitionCount = Integer.parseInt(viewParameters.get(ViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name()));
    this.props.put(PARTITION_COUNT, viewPartitionCount);
    viewPartitioner = Lazy.of(() -> {
      String viewPartitionerClass = this.viewParameters.get(ViewParameters.MATERIALIZED_VIEW_PARTITIONER.name());
      String viewPartitionerParamsString =
          this.viewParameters.get(ViewParameters.MATERIALIZED_VIEW_PARTITIONER_PARAMS.name());
      return PartitionUtils.getVenicePartitioner(viewPartitionerClass, viewPartitionerParamsString);
    });
  }

  @Override
  public Map<String, VeniceProperties> getTopicNamesAndConfigsForVersion(int version) {
    VeniceProperties properties = new VeniceProperties(props);
    String viewName = viewParameters.get(ViewParameters.MATERIALIZED_VIEW_NAME.name());
    return Collections.singletonMap(
        Version.composeKafkaTopic(store.getName(), version) + VIEW_TOPIC_SEPARATOR + viewName
            + MATERIALIZED_VIEW_TOPIC_SUFFIX,
        properties);
  }

  /**
   * {@link ViewParameters#MATERIALIZED_VIEW_PARTITION_COUNT} is required to configure a new re-partition view.
   * {@link ViewParameters#MATERIALIZED_VIEW_PARTITIONER} is optional. The re-partition view will use the store level
   * partitioner config if it's not specified in the view parameters.
   * {@link ViewParameters#MATERIALIZED_VIEW_PARTITIONER_PARAMS} is optional.
   */
  @Override
  public void validateConfigs() {
    String viewName = viewParameters.get(ViewParameters.MATERIALIZED_VIEW_NAME.name());
    if (viewName == null) {
      throw new VeniceException(String.format(MISSING_PARAMETER_MESSAGE, ViewParameters.MATERIALIZED_VIEW_NAME.name()));
    }
    if (store.getViewConfigs().containsKey(viewName)) {
      throw new VeniceException("A view config with the same view name already exist, view name: " + viewName);
    }
    String viewPartitioner = viewParameters.get(ViewParameters.MATERIALIZED_VIEW_PARTITIONER.name());
    if (viewPartitioner == null) {
      throw new VeniceException(
          String.format(MISSING_PARAMETER_MESSAGE, ViewParameters.MATERIALIZED_VIEW_PARTITIONER.name()));
    }
    try {
      Class.forName(viewPartitioner);
    } catch (ClassNotFoundException e) {
      throw new VeniceException("Cannot find partitioner class: " + viewPartitioner);
    }
    String partitionCountString = viewParameters.get(ViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name());
    if (partitionCountString == null) {
      throw new VeniceException(
          String.format(MISSING_PARAMETER_MESSAGE, ViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name()));
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
            viewConfig.getViewParameters().get(ViewParameters.MATERIALIZED_VIEW_PARTITIONER.name());
        int configPartitionCount = Integer
            .parseInt(viewConfig.getViewParameters().get(ViewParameters.MATERIALIZED_VIEW_PARTITION_COUNT.name()));
        if (configPartitionCount == viewPartitionCount && configPartitioner.equals(viewPartitioner)) {
          throw new VeniceException(
              "A view with identical view configs already exist, view name: " + viewConfigEntries.getKey());
        }
      }
    }
  }

  @Override
  public String getWriterClassName() {
    return MATERIALIZED_VIEW_WRITER_CLASS_NAME;
  }

  public int getViewPartitionCount() {
    return viewPartitionCount;
  }

  public VenicePartitioner getViewPartitioner() {
    return viewPartitioner.get();
  }
}
