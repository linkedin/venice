package com.linkedin.venice.meta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public enum MaterializedViewParameters {
  /**
   * Parameter key used to specify the re-partition view name.
   */
  MATERIALIZED_VIEW_NAME,
  /**
   * Parameter key used to specify the partitioner for the re-partition view.
   */
  MATERIALIZED_VIEW_PARTITIONER,
  /**
   * Parameter key used to specify the partitioner parameters for the partitioner associated with the re-partition view.
   */
  MATERIALIZED_VIEW_PARTITIONER_PARAMS,
  /**
   * Parameter key used to specify the partition count for the re-partition view.
   */
  MATERIALIZED_VIEW_PARTITION_COUNT,
  /**
   * Parameter key used to specify the top level fields to perform filtering on for records to be produced to the view.
   */
  MATERIALIZED_VIEW_FILTER_BY_FIELDS,
  /**
   * Parameter key used to specify the top level fields to perform projection on for records in the materialized view.
   */
  MATERIALIZED_VIEW_PROJECTION_FIELDS,

  /**
   * Parameter key used to persist the generated projection schema. This is meant for internal use and purposely not
   * exposed to the {@link Builder}
   */
  MATERIALIZED_VIEW_PROJECTION_SCHEMA;

  public static class Builder {
    private final String viewName;
    private String partitioner;
    private String partitionerParams;
    private String partitionCount;
    private List<String> filterByFields = Collections.emptyList();
    private List<String> projectionFields = Collections.emptyList();

    public Builder(String viewName) {
      this.viewName = Objects.requireNonNull(viewName, "View name cannot be null for ViewParameters");
    }

    public Builder(String viewName, Map<String, String> viewParams) {
      this.viewName = viewName;
      this.partitioner = viewParams.get(MATERIALIZED_VIEW_PARTITIONER.name());
      this.partitionerParams = viewParams.get(MATERIALIZED_VIEW_PARTITIONER_PARAMS.name());
      this.partitionCount = viewParams.get(MATERIALIZED_VIEW_PARTITION_COUNT.name());
      String filteringFieldsString = viewParams.get(MATERIALIZED_VIEW_FILTER_BY_FIELDS.name());
      if (filteringFieldsString != null) {
        this.filterByFields =
            parsePropertyStringToList(filteringFieldsString, MATERIALIZED_VIEW_FILTER_BY_FIELDS.name());
      }
      String projectionFieldsString = viewParams.get(MATERIALIZED_VIEW_PROJECTION_FIELDS.name());
      if (projectionFieldsString != null) {
        this.projectionFields =
            parsePropertyStringToList(projectionFieldsString, MATERIALIZED_VIEW_PROJECTION_FIELDS.name());
      }
    }

    public Builder setPartitioner(String partitioner) {
      this.partitioner = partitioner;
      return this;
    }

    public Builder setPartitionerParams(String partitionerParams) {
      this.partitionerParams = partitionerParams;
      return this;
    }

    public Builder setPartitionerParams(Map<String, String> partitionerParams) {
      try {
        this.partitionerParams = ObjectMapperFactory.getInstance().writeValueAsString(partitionerParams);
      } catch (JsonProcessingException e) {
        throw new VeniceException("Failed to flat the partitioner params map", e);
      }
      return this;
    }

    public Builder setPartitionCount(int partitionCount) {
      this.partitionCount = Integer.toString(partitionCount);
      return this;
    }

    public Builder setFilterByFields(List<String> filterByFields) {
      this.filterByFields = filterByFields;
      return this;
    }

    public Builder setProjectionFields(List<String> projectionFields) {
      this.projectionFields = projectionFields;
      return this;
    }

    public Map<String, String> build() {
      Map<String, String> viewParams = new HashMap<>();
      viewParams.put(MATERIALIZED_VIEW_NAME.name(), viewName);
      if (partitioner != null) {
        viewParams.put(MATERIALIZED_VIEW_PARTITIONER.name(), partitioner);
      }
      if (partitionerParams != null) {
        viewParams.put(MATERIALIZED_VIEW_PARTITIONER_PARAMS.name(), partitionerParams);
      }
      if (partitionCount != null) {
        viewParams.put(MATERIALIZED_VIEW_PARTITION_COUNT.name(), partitionCount);
      }
      if (!filterByFields.isEmpty()) {
        viewParams.put(
            MATERIALIZED_VIEW_FILTER_BY_FIELDS.name(),
            convertListToStringProperty(filterByFields, MATERIALIZED_VIEW_FILTER_BY_FIELDS.name()));
      }
      if (!projectionFields.isEmpty()) {
        viewParams.put(
            MATERIALIZED_VIEW_PROJECTION_FIELDS.name(),
            convertListToStringProperty(projectionFields, MATERIALIZED_VIEW_PROJECTION_FIELDS.name()));
      }
      return viewParams;
    }
  }

  public static String convertListToStringProperty(List<String> list, String propertyName) {
    try {
      return ObjectMapperFactory.getInstance().writeValueAsString(list);
    } catch (JsonProcessingException e) {
      throw new VeniceException("Failed to convert list to a string property for property: " + propertyName);
    }
  }

  public static List<String> parsePropertyStringToList(String propertyString, String propertyName) {
    try {
      return ObjectMapperFactory.getInstance().readValue(propertyString, List.class);
    } catch (JsonProcessingException e) {
      throw new VeniceException(
          "Failed to parse the property string to list for property: " + propertyName + ", property string: "
              + propertyString);
    }
  }
}
