package com.linkedin.venice.client.consumer;

import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.schema.SchemaReader;
import java.util.Properties;
import org.apache.avro.specific.SpecificRecord;


public class ChangelogClientConfig<T extends SpecificRecord> {
  private Properties consumerProperties;
  private SchemaReader schemaReader;
  private String viewClassName;
  private ClientConfig<T> innerClientConfig;
  private D2ControllerClient d2ControllerClient;
  private String clusterName;

  public ChangelogClientConfig(String storeName) {
    this.innerClientConfig = new ClientConfig<>(storeName);
  }

  public ChangelogClientConfig() {
    this.innerClientConfig = new ClientConfig<>();
  }

  public ChangelogClientConfig<T> setStoreName(String storeName) {
    this.innerClientConfig.setStoreName(storeName);
    return this;
  }

  public String getStoreName() {
    return innerClientConfig.getStoreName();
  }

  public ChangelogClientConfig<T> setConsumerProperties(Properties consumerProperties) {
    this.consumerProperties = consumerProperties;
    return this;
  }

  public Properties getConsumerProperties() {
    return consumerProperties;
  }

  public ChangelogClientConfig<T> setSchemaReader(SchemaReader schemaReader) {
    this.schemaReader = schemaReader;
    return this;
  }

  public SchemaReader getSchemaReader() {
    return schemaReader;
  }

  public ChangelogClientConfig<T> setViewClassName(String viewClassName) {
    this.viewClassName = viewClassName;
    return this;
  }

  public String getViewClassName() {
    return viewClassName;
  }

  public ChangelogClientConfig<T> setD2ServiceName(String d2ServiceName) {
    this.innerClientConfig.setD2ServiceName(d2ServiceName);
    return this;
  }

  public String getD2ServiceName() {
    return this.innerClientConfig.getD2ServiceName();
  }

  public ChangelogClientConfig<T> setD2ControllerClient(D2ControllerClient d2ControllerClient) {
    this.d2ControllerClient = d2ControllerClient;
    return this;
  }

  public D2ControllerClient getD2ControllerClient() {
    return this.d2ControllerClient;
  }

  public ChangelogClientConfig<T> setVeniceURL(String veniceURL) {
    this.innerClientConfig.setVeniceURL(veniceURL);
    return this;
  }

  public String getVeniceURL() {
    return this.innerClientConfig.getVeniceURL();
  }

  public ClientConfig<T> getInnerClientConfig() {
    return this.innerClientConfig;
  }

  public ChangelogClientConfig<T> setClusterName(String clusterName) {
    this.clusterName = clusterName;
    return this;
  }

  public String getClusterName() {
    return this.clusterName;
  }

  public static <V extends SpecificRecord> ChangelogClientConfig<V> cloneConfig(ChangelogClientConfig<V> config) {
    ChangelogClientConfig<V> newConfig = new ChangelogClientConfig<V>().setStoreName(config.getStoreName())
        .setVeniceURL(config.getVeniceURL())
        .setD2ServiceName(config.getD2ServiceName())
        .setConsumerProperties(config.getConsumerProperties())
        .setSchemaReader(config.getSchemaReader())
        .setClusterName(config.getClusterName())
        .setViewClassName(config.getViewClassName())
        .setD2ControllerClient(config.getD2ControllerClient());
    return newConfig;
  }
}
