package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.linkedin.venice.systemstore.schemas.StoreETLConfig;


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

  ETLStoreConfig clone();
}
