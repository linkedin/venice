package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.linkedin.venice.systemstore.schemas.DataRecoveryConfig;


/**
 * This interface defines all the public APIs, and if you need to add accessors to
 * some new fields, this interface needs to be changed accordingly.
 */
@JsonDeserialize(as = DataRecoveryVersionConfigImpl.class)
public interface DataRecoveryVersionConfig extends DataModelBackedStructure<DataRecoveryConfig> {
  String getDataRecoverySourceFabric();

  void setDataRecoverySourceFabric(String dataRecoverySourceFabric);

  boolean isDataRecoveryComplete();

  void setDataRecoveryComplete(boolean dataRecoveryComplete);

  int getDataRecoverySourceVersionNumber();

  void setDataRecoverySourceVersionNumber(int dataRecoverySourceVersionNumber);

  DataRecoveryVersionConfig clone();
}
