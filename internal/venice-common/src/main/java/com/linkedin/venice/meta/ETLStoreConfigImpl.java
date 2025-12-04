package com.linkedin.venice.meta;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.venice.systemstore.schemas.StoreETLConfig;
import com.linkedin.venice.utils.AvroCompatibilityUtils;
import java.util.Objects;


/**
 * A container of ETL Enabled Store related configurations.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ETLStoreConfigImpl implements ETLStoreConfig {
  /**
   * The internal data model for {@link ETLStoreConfig}.
   */
  private final StoreETLConfig etlConfig;

  public ETLStoreConfigImpl(
      @JsonProperty("etledUserProxyAccount") String etledUserProxyAccount,
      @JsonProperty("regularVersionETLEnabled") boolean regularVersionETLEnabled,
      @JsonProperty("futureVersionETLEnabled") boolean futureVersionETLEnabled,
      @JsonProperty("etlStrategy") int etlStrategy) {
    this.etlConfig = new StoreETLConfig();
    this.etlConfig.etledUserProxyAccount = etledUserProxyAccount;
    this.etlConfig.regularVersionETLEnabled = regularVersionETLEnabled;
    this.etlConfig.futureVersionETLEnabled = futureVersionETLEnabled;
    this.etlConfig.etlStrategy = etlStrategy == 0 ? VeniceETLStrategy.EXTERNAL_SERVICE.getValue() : etlStrategy;
  }

  public ETLStoreConfigImpl() {
    this("", false, false, VeniceETLStrategy.EXTERNAL_SERVICE.getValue());
  }

  ETLStoreConfigImpl(StoreETLConfig config) {
    this.etlConfig = config;
  }

  @Override
  public String getEtledUserProxyAccount() {
    return this.etlConfig.etledUserProxyAccount.toString();
  }

  @Override
  public void setEtledUserProxyAccount(String etledUserProxyAccount) {
    this.etlConfig.etledUserProxyAccount = etledUserProxyAccount;
  }

  @Override
  public boolean isRegularVersionETLEnabled() {
    return this.etlConfig.regularVersionETLEnabled;
  }

  @Override
  public void setRegularVersionETLEnabled(boolean regularVersionETLEnabled) {
    this.etlConfig.regularVersionETLEnabled = regularVersionETLEnabled;
  }

  @Override
  public boolean isFutureVersionETLEnabled() {
    return this.etlConfig.futureVersionETLEnabled;
  }

  @Override
  public void setFutureVersionETLEnabled(boolean futureVersionETLEnabled) {
    this.etlConfig.futureVersionETLEnabled = futureVersionETLEnabled;
  }

  @Override
  public VeniceETLStrategy getETLStrategy() {
    return VeniceETLStrategy.getVeniceETLStrategyFromInt(etlConfig.etlStrategy);
  }

  @Override
  public void setETLStrategy(VeniceETLStrategy etlStrategy) {
    this.etlConfig.etlStrategy = etlStrategy.getValue();
  }

  @Override
  public StoreETLConfig dataModel() {
    return this.etlConfig;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ETLStoreConfigImpl that = (ETLStoreConfigImpl) o;
    return AvroCompatibilityUtils.compare(etlConfig, that.etlConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(etlConfig);
  }

  @JsonIgnore
  public ETLStoreConfig clone() {
    return new ETLStoreConfigImpl(
        getEtledUserProxyAccount(),
        isRegularVersionETLEnabled(),
        isFutureVersionETLEnabled(),
        getETLStrategy().getValue());
  }
}
