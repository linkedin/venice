package com.linkedin.venice.meta;

import com.linkedin.venice.systemstore.schemas.StoreETLConfig;
import java.util.Objects;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * A container of ETL Enabled Store related configurations.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ETLStoreConfig implements DataModelBackedStructure<StoreETLConfig> {
  /**
   * The internal data model for {@link ETLStoreConfig}.
   */
  private final StoreETLConfig etlConfig;

  public ETLStoreConfig(
      @JsonProperty("etledUserProxyAccount") String etledUserProxyAccount,
      @JsonProperty("regularVersionETLEnabled") boolean regularVersionETLEnabled,
      @JsonProperty("futureVersionETLEnabled") boolean futureVersionETLEnabled
  ) {
    this.etlConfig = new StoreETLConfig();
    this.etlConfig.etledUserProxyAccount = etledUserProxyAccount;
    this.etlConfig.regularVersionETLEnabled = regularVersionETLEnabled;
    this.etlConfig.futureVersionETLEnabled = futureVersionETLEnabled;
  }

  public ETLStoreConfig() {
    this("", false, false);
  }

  ETLStoreConfig(StoreETLConfig config) {
    this.etlConfig = config;
  }

  public String getEtledUserProxyAccount() {
    return this.etlConfig.etledUserProxyAccount.toString();
  }

  public void setEtledUserProxyAccount(String etledUserProxyAccount) {
    this.etlConfig.etledUserProxyAccount = etledUserProxyAccount;
  }

  public boolean isRegularVersionETLEnabled() {
    return this.etlConfig.regularVersionETLEnabled;
  }

  public void setRegularVersionETLEnabled(boolean regularVersionETLEnabled) {
    this.etlConfig.regularVersionETLEnabled = regularVersionETLEnabled;
  }

  public boolean isFutureVersionETLEnabled() {
    return this.etlConfig.futureVersionETLEnabled;
  }

  public void setFutureVersionETLEnabled(boolean futureVersionETLEnabled) {
    this.etlConfig.futureVersionETLEnabled = futureVersionETLEnabled;
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
    ETLStoreConfig that = (ETLStoreConfig) o;
    return etlConfig.equals(that.etlConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(etlConfig);
  }

  @JsonIgnore
  public ETLStoreConfig clone(){
    return new ETLStoreConfig(getEtledUserProxyAccount(), isRegularVersionETLEnabled(), isFutureVersionETLEnabled());
  }
}
