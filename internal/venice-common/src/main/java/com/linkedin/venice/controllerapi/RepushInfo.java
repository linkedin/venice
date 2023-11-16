package com.linkedin.venice.controllerapi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.linkedin.venice.meta.Version;


@JsonIgnoreProperties(ignoreUnknown = true)
public class RepushInfo {
  private String kafkaBrokerUrl;
  private Version version;
  private String systemSchemaClusterD2ServiceName;
  private String systemSchemaClusterD2ZkHost;

  public static RepushInfo createRepushInfo(
      Version version,
      String kafkaBrokerUrl,
      String systemSchemaClusterD2ServiceName,
      String systemSchemaClusterD2ZkHost) {
    RepushInfo repushInfo = new RepushInfo();
    repushInfo.setVersion(version);
    repushInfo.setKafkaBrokerUrl(kafkaBrokerUrl);
    repushInfo.setSystemSchemaClusterD2ServiceName(systemSchemaClusterD2ServiceName);
    repushInfo.setSystemSchemaClusterD2ZkHost(systemSchemaClusterD2ZkHost);
    return repushInfo;
  }

  public void setVersion(Version version) {
    this.version = version;
  }

  public void setKafkaBrokerUrl(String kafkaBrokerUrl) {
    this.kafkaBrokerUrl = kafkaBrokerUrl;
  }

  public void setSystemSchemaClusterD2ServiceName(String systemSchemaClusterD2ServiceName) {
    this.systemSchemaClusterD2ServiceName = systemSchemaClusterD2ServiceName;
  }

  public void setSystemSchemaClusterD2ZkHost(String systemSchemaClusterD2ZkHost) {
    this.systemSchemaClusterD2ZkHost = systemSchemaClusterD2ZkHost;
  }

  public String getKafkaBrokerUrl() {
    return this.kafkaBrokerUrl;
  }

  public Version getVersion() {
    return version;
  }

  public String getSystemSchemaClusterD2ServiceName() {
    return systemSchemaClusterD2ServiceName;
  }

  public String getSystemSchemaClusterD2ZkHost() {
    return systemSchemaClusterD2ZkHost;
  }
}
