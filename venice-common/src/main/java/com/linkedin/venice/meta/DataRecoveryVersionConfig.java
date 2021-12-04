package com.linkedin.venice.meta;

import com.linkedin.venice.systemstore.schemas.DataRecoveryConfig;
import org.codehaus.jackson.map.annotate.JsonDeserialize;


/**
 * This interface defines all the public APIs, and if you need to add accessors to
 * some new fields, this interface needs to be changed accordingly.
 */
@JsonDeserialize(as = DataRecoveryVersionConfigImpl.class)
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(as = DataRecoveryVersionConfigImpl.class)
public interface DataRecoveryVersionConfig extends DataModelBackedStructure<DataRecoveryConfig> {

  String getDataRecoverySourceFabric();

  void setDataRecoverySourceFabric(String dataRecoverySourceFabric);

  boolean isDataRecoveryComplete();

  void setDataRecoveryComplete(boolean dataRecoveryComplete);

  DataRecoveryVersionConfig clone();
}
