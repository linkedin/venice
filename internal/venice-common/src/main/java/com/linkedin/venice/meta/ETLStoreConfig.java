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

  /**
   * SLA tier the external ETL service should use when it first onboards this store in a fabric. It is sent with
   * every onboard request and never synced back from the external ETL service, so it does not reflect the store's
   * current tier. {@code null} means not set.
   */
  String getInitialSlaTier();

  void setInitialSlaTier(String initialSlaTier);

  /**
   * Users the external ETL service should grant access to when it first onboards this store in a fabric. Sent with
   * every onboard request and never synced back from the external ETL service. {@code null} means not set.
   */
  List<String> getInitialGrantAclUsers();

  void setInitialGrantAclUsers(List<String> initialGrantAclUsers);

  /**
   * ETL workload type the external ETL service should use when it first onboards this store in a fabric. Unrelated
   * to the store-level {@link Store#getWorkloadType()}. Sent with every onboard request and never synced back from
   * the external ETL service. {@code null} means not set.
   */
  String getInitialWorkloadType();

  void setInitialWorkloadType(String initialWorkloadType);

  ETLStoreConfig clone();
}
