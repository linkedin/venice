package com.linkedin.venice.exceptions;

public class MissingKeyInStoreMetadataException extends VeniceException {
  private final String keyName;
  private final String metadataClassName;

  public MissingKeyInStoreMetadataException(String keyName, String metadataClassName) {
    super("Get null " + metadataClassName + " value for key " + keyName + " in metadata store.");
    this.keyName = keyName;
    this.metadataClassName = metadataClassName;
  }

  public MissingKeyInStoreMetadataException(String keyName, String metadataClassName, Throwable t) {
    super("Get null " + metadataClassName + " value for key " + keyName + " in metadata store.", t);
    this.keyName = keyName;
    this.metadataClassName = metadataClassName;
  }

  public String getKeyName() {
    return keyName;
  }

  public String getMetadataClassName() {
    return metadataClassName;
  }

}
