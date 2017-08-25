package com.linkedin.venice.exceptions;

public class VeniceIngestionTaskKilledException extends VeniceException {
  public VeniceIngestionTaskKilledException(String topic) {
    super("Ingestion task for topic: " + topic + " is killed.");
  }

  public VeniceIngestionTaskKilledException(String topic, Throwable e) {
    super("Ingestion task for topic: " + topic + " is killed.", e);
  }
}
