package com.linkedin.venice.meta;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * A container of ETL Enabled Store related configurations.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ETLStoreConfig {

  /**
   * If enabled regular ETL or future version ETL, this account name is part of path for
   * where the ETLed snapshots will go. for example, for user account veniceetl001,
   * snapshots will be published to HDFS /jobs/veniceetl001/storeName
   */
  private String etledUserProxyAccount;

  /**
   * Whether or not enable regular version ETL for this store.
   */
  private boolean regularVersionETLEnabled;

  /**
   * Whether or not enable future version ETL - the version that might come online in future - for this store.
   */
  private boolean futureVersionETLEnabled;

  public ETLStoreConfig(
      @JsonProperty("etledUserProxyAccount") String etledUserProxyAccount,
      @JsonProperty("regularVersionETLEnabled") boolean regularVersionETLEnabled,
      @JsonProperty("futureVersionETLEnabled") boolean futureVersionETLEnabled
  ) {
    this.etledUserProxyAccount = etledUserProxyAccount;
    this.regularVersionETLEnabled = regularVersionETLEnabled;
    this.futureVersionETLEnabled = futureVersionETLEnabled;
  }

  public ETLStoreConfig() {
    this("", false, false);
  }
  public String getEtledUserProxyAccount() {
    return etledUserProxyAccount;
  }

  public void setEtledUserProxyAccount(String etledUserProxyAccount) {
    this.etledUserProxyAccount = etledUserProxyAccount;
  }

  public boolean isRegularVersionETLEnabled() {
    return regularVersionETLEnabled;
  }

  public void setRegularVersionETLEnabled(boolean regularVersionETLEnabled) {
    this.regularVersionETLEnabled = regularVersionETLEnabled;
  }

  public boolean isFutureVersionETLEnabled() {
    return futureVersionETLEnabled;
  }

  public void setFutureVersionETLEnabled(boolean futureVersionETLEnabled) {
    this.futureVersionETLEnabled = futureVersionETLEnabled;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ETLStoreConfig that = (ETLStoreConfig) o;
    if (!etledUserProxyAccount.equals(that.getEtledUserProxyAccount())) return false;
    if (!regularVersionETLEnabled == that.regularVersionETLEnabled) return false;
    return futureVersionETLEnabled == that.futureVersionETLEnabled;
  }

  @Override
  public int hashCode() {
    int result = etledUserProxyAccount.hashCode();
    result = 31 * result + (regularVersionETLEnabled ? 1 : 0);
    result = 31 * result + (futureVersionETLEnabled ? 1 : 0);
    return result;
  }

  @JsonIgnore
  public ETLStoreConfig clone(){
    return new ETLStoreConfig(etledUserProxyAccount, regularVersionETLEnabled, futureVersionETLEnabled);
  }
}
