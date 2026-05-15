package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.linkedin.venice.systemstore.schemas.StoreETLConfig;
import java.util.List;


/**
 * This interface defines all the public APIs, and if you need to add accessors to
 * some new fields, this interface needs to be changed accordingly.
 */
@JsonDeserialize(as = ETLStoreConfigImpl.class)
public interface ETLStoreConfig extends DataModelBackedStructure<StoreETLConfig> {
  String getEtledUserProxyAccount();

  void setEtledUserProxyAccount(String etledUserProxyAccount);

  boolean isRegularVersionETLEnabled();

  void setRegularVersionETLEnabled(boolean regularVersionETLEnabled);

  boolean isFutureVersionETLEnabled();

  void setFutureVersionETLEnabled(boolean futureVersionETLEnabled);

  VeniceETLStrategy getETLStrategy();

  void setETLStrategy(VeniceETLStrategy etlStrategy);

  /**
   * Allowlist of fabric names where the controller fires onboardETL / offboardETL on the
   * ExternalETLService. {@code null} means "no restriction; fire in every fabric" (default
   * behavior). A non-empty list restricts firing to only the listed fabrics. An empty list is
   * rejected by the parent controller validator; to disable ETL across all fabrics, set
   * {@link #setRegularVersionETLEnabled} and {@link #setFutureVersionETLEnabled} to false instead.
   */
  List<String> getEtlActiveFabrics();

  void setEtlActiveFabrics(List<String> etlActiveFabrics);

  ETLStoreConfig clone();
}
