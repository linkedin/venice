package com.linkedin.venice.meta;

import com.linkedin.venice.systemstore.schemas.DataRecoveryConfig;
import com.linkedin.venice.utils.AvroCompatibilityUtils;
import java.util.Objects;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;


@JsonIgnoreProperties(ignoreUnknown = true)
public class DataRecoveryVersionConfigImpl implements DataRecoveryVersionConfig {
  private final DataRecoveryConfig dataRecoveryConfig;

  public DataRecoveryVersionConfigImpl(
      @JsonProperty("dataRecoverySourceFabric") String dataRecoverySourceFabric,
      @JsonProperty("isDataRecoveryComplete") boolean isDataRecoveryComplete) {
    dataRecoveryConfig = new DataRecoveryConfig();
    dataRecoveryConfig.dataRecoverySourceFabric = dataRecoverySourceFabric;
    dataRecoveryConfig.isDataRecoveryComplete = isDataRecoveryComplete;
  }

  DataRecoveryVersionConfigImpl(DataRecoveryConfig dataRecoveryConfig) {
    this.dataRecoveryConfig = dataRecoveryConfig;
  }

  @Override
  public DataRecoveryConfig dataModel() {
    return dataRecoveryConfig;
  }

  @Override
  public String getDataRecoverySourceFabric() {
    return dataRecoveryConfig.dataRecoverySourceFabric.toString();
  }

  @Override
  public void setDataRecoverySourceFabric(String dataRecoverySourceFabric) {
    dataRecoveryConfig.dataRecoverySourceFabric = dataRecoverySourceFabric;
  }

  @Override
  public boolean isDataRecoveryComplete() {
    return dataRecoveryConfig.isDataRecoveryComplete;
  }

  @Override
  public void setDataRecoveryComplete(boolean dataRecoveryComplete) {
    dataRecoveryConfig.isDataRecoveryComplete = dataRecoveryComplete;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataRecoveryVersionConfigImpl that = (DataRecoveryVersionConfigImpl) o;
    return AvroCompatibilityUtils.compare(dataRecoveryConfig, that.dataRecoveryConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataRecoveryConfig);
  }

  @Override
  public DataRecoveryVersionConfig clone() {
    return new DataRecoveryVersionConfigImpl(getDataRecoverySourceFabric(), isDataRecoveryComplete());
  }
}
